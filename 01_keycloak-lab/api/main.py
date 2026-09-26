import os

import httpx

from fastapi import (
    Depends,
    FastAPI,
    HTTPException,
    status,
)

from fastapi.security import (
    HTTPAuthorizationCredentials,
    HTTPBearer,
)

from jose import jwt
from jose.exceptions import JWTError


# ============================================================
# 1. Configuration
# ============================================================

# ------------------------------------------------------------
# Docker 내부에서 Keycloak에 접근할 때 사용하는 URL
#
# FastAPI 컨테이너
#       |
#       | http://keycloak:8080
#       v
# Keycloak 컨테이너
# ------------------------------------------------------------

KEYCLOAK_INTERNAL_URL = os.getenv(
    "KEYCLOAK_INTERNAL_URL",
    "http://keycloak:8080"
)


# ------------------------------------------------------------
# JWT의 "iss" claim 검증에 사용하는 외부 Issuer
#
# Windows 브라우저 / PowerShell에서 접근하는 주소
#
# JWT:
#   "iss": "http://localhost:8080/realms/hdaic"
#
# ------------------------------------------------------------

KEYCLOAK_ISSUER = os.getenv(
    "KEYCLOAK_ISSUER",
    "http://localhost:8080/realms/hdaic"
)


# ------------------------------------------------------------
# Keycloak Realm
# ------------------------------------------------------------

KEYCLOAK_REALM = os.getenv(
    "KEYCLOAK_REALM",
    "hdaic"
)


# ------------------------------------------------------------
# JWT Audience
# ------------------------------------------------------------

KEYCLOAK_CLIENT_ID = os.getenv(
    "KEYCLOAK_CLIENT_ID",
    "usage-api"
)


# ============================================================
# 2. Keycloak Internal URLs
# ============================================================

# Docker 내부에서 접근하는 Realm URL
#
# http://keycloak:8080/realms/hdaic
#
REALM_INTERNAL_URL = (
    f"{KEYCLOAK_INTERNAL_URL}"
    f"/realms/{KEYCLOAK_REALM}"
)


# Docker 내부 OIDC Discovery URL
#
# http://keycloak:8080/realms/hdaic/
#     .well-known/openid-configuration
#
OIDC_CONFIG_URL = (
    f"{REALM_INTERNAL_URL}"
    f"/.well-known/openid-configuration"
)


# Docker 내부 JWKS URL
#
# 중요:
#
# OIDC discovery 결과의 jwks_uri를 그대로 사용하지 않습니다.
#
# Keycloak의 외부 hostname이 localhost이기 때문에
# discovery 결과가 다음과 같이 나올 수 있습니다.
#
# http://localhost:8080/realms/hdaic/protocol/openid-connect/certs
#
# 하지만 FastAPI 컨테이너에서 localhost는
# Keycloak 컨테이너가 아닙니다.
#
# 따라서 Docker 내부 주소를 직접 구성합니다.
#
JWKS_URL = (
    f"{REALM_INTERNAL_URL}"
    f"/protocol/openid-connect/certs"
)


# ============================================================
# 3. FastAPI
# ============================================================

app = FastAPI(
    title="Keycloak JWT Demo"
)


# Authorization: Bearer <token>
security = HTTPBearer()


# ============================================================
# 4. OIDC Discovery
# ============================================================

async def get_oidc_config():
    """
    Keycloak OIDC Discovery 정보를 가져옵니다.

    Docker 내부에서 Keycloak에 접근합니다.

    FastAPI
       |
       | http://keycloak:8080
       v
    Keycloak
    """

    try:

        async with httpx.AsyncClient() as client:

            response = await client.get(
                OIDC_CONFIG_URL,
                timeout=5.0
            )

            response.raise_for_status()

            return response.json()

    except httpx.HTTPError as e:

        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "Unable to connect to Keycloak OIDC endpoint: "
                f"{str(e)}"
            )
        )


# ============================================================
# 5. Get Keycloak Public Keys
# ============================================================

async def get_jwks():
    """
    Keycloak의 JWKS(public keys)를 가져옵니다.

    JWT는 Keycloak의 private key로 서명되고,
    FastAPI는 public key로 signature를 검증합니다.

    중요:
    discovery에서 받은 jwks_uri를 사용하지 않고
    Docker 내부 Keycloak URL을 직접 사용합니다.
    """

    try:

        async with httpx.AsyncClient() as client:

            response = await client.get(
                JWKS_URL,
                timeout=5.0
            )

            response.raise_for_status()

            return response.json()

    except httpx.HTTPError as e:

        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "Unable to connect to Keycloak JWKS endpoint: "
                f"{str(e)}"
            )
        )


# ============================================================
# 6. Verify JWT
# ============================================================

async def verify_token(token: str):
    """
    JWT를 검증합니다.

    검증 항목:

    1. JWT header의 kid
    2. Keycloak public key
    3. Signature
    4. Algorithm = RS256
    5. Issuer
    6. Audience
    """

    try:

        # ----------------------------------------------------
        # JWT Header 확인
        # ----------------------------------------------------

        header = jwt.get_unverified_header(token)

        kid = header.get("kid")

        if not kid:

            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="JWT kid is missing"
            )


        # ----------------------------------------------------
        # Keycloak JWKS 가져오기
        # ----------------------------------------------------

        jwks = await get_jwks()


        # ----------------------------------------------------
        # JWT의 kid와 동일한 public key 찾기
        # ----------------------------------------------------

        key = None

        for jwk in jwks.get("keys", []):

            if jwk.get("kid") == kid:

                key = jwk

                break


        if key is None:

            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=(
                    "Unable to find matching Keycloak public key"
                )
            )


        # ----------------------------------------------------
        # JWT 검증
        # ----------------------------------------------------

        payload = jwt.decode(
            token,
            key,
            algorithms=["RS256"],

            # JWT의 iss 검증
            #
            # http://localhost:8080/realms/hdaic
            #
            issuer=KEYCLOAK_ISSUER,

            # JWT의 aud 검증
            #
            # usage-api
            #
            audience=KEYCLOAK_CLIENT_ID
        )


        return payload


    except HTTPException:

        raise


    except JWTError as e:

        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid JWT: {str(e)}"
        )


    except Exception as e:

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"JWT verification error: {str(e)}"
        )


# ============================================================
# 7. Current User
# ============================================================

async def get_current_user(
    credentials: HTTPAuthorizationCredentials =
        Depends(security)
):
    """
    Authorization header에서 JWT를 가져오고
    JWT를 검증합니다.
    """

    token = credentials.credentials

    payload = await verify_token(token)

    return payload


# ============================================================
# 8. Public API
# ============================================================

@app.get("/public")
async def public():

    return {
        "message": "This endpoint is public"
    }


# ============================================================
# 9. Protected API
# ============================================================

@app.get("/me")
async def me(
    user=Depends(get_current_user)
):
    """
    JWT에 포함된 사용자 정보를 반환합니다.
    """

    return {
        "user_id": user.get("sub"),

        "username": user.get(
            "preferred_username"
        ),

        "email": user.get(
            "email"
        ),

        "name": user.get(
            "name"
        ),

        "roles": user.get(
            "realm_access",
            {}
        ).get(
            "roles",
            []
        )
    }
