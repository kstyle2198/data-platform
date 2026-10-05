네. 최근 진행한 **OpenLDAP ↔ Keycloak 연동 실습**을 현재 프로젝트 기준으로 정리하면 아래와 같습니다.

## 1. 전체 구성

현재 `D:\data_platform\03_iceberg` Docker 환경에서 다음 구조를 만들고 있습니다.

```text
                    ┌─────────────────────┐
                    │      Browser UI      │
                    │  index.html         │
                    └──────────┬──────────┘
                               │
                         JWT / Login
                               │
                               ▼
                    ┌─────────────────────┐
                    │      Keycloak       │
                    │   Realm: hdaic      │
                    └──────────┬──────────┘
                               │
                       LDAP Federation
                               │
                               ▼
                    ┌─────────────────────┐
                    │      OpenLDAP       │
                    │  dc=example,dc=org  │
                    │                     │
                    │  ou=people          │
                    └─────────────────────┘
```

Keycloak은 자체 사용자 DB만 사용하는 것이 아니라 **User Federation을 통해 OpenLDAP을 외부 사용자 저장소로 연결**할 수 있습니다. LDAP 사용자의 username, email, first name, last name 등의 속성도 Keycloak 사용자 모델에 매핑할 수 있습니다. ([GitHub][1])

---

# 2. OpenLDAP 구성

Docker Compose에 OpenLDAP을 추가했습니다.

주요 구성은 다음과 같습니다.

```text
openldap
 ├─ LDAP Port : 389
 ├─ Base DN   : dc=example,dc=org
 ├─ Admin DN  : cn=admin,dc=example,dc=org
 └─ Users
     └─ ou=people,dc=example,dc=org
```

그리고 LDAP 데이터의 영속성을 위해 Docker volume을 사용했습니다.

```text
03_iceberg_openldap_data
        ↓
/var/lib/ldap

03_iceberg_openldap_config
        ↓
/etc/ldap/slapd.d
```

따라서

```bash
docker compose down
docker compose up -d
```

를 해도 LDAP 사용자 데이터가 유지되는 구조입니다.

---

# 3. OpenLDAP 직접 확인

LDAP 서버가 실제로 사용자 정보를 가지고 있는지 확인하기 위해 `ldapsearch`도 테스트했습니다.

예를 들어:

```bash
docker exec openldap ldapsearch \
  -x \
  -H ldap://localhost:389 \
  -D "cn=admin,dc=example,dc=org" \
  -W \
  -b "dc=example,dc=org"
```

초기에 다음과 같은 문제가 있었습니다.

```text
ldap_bind: Server is unwilling to perform (53)
additional info: unauthenticated bind
```

그리고 최근에는 OpenLDAP의 persistent configuration과 관련하여:

```text
LDAP_CONFIG_PASSWORD
```

환경변수가 설정되지 않은 문제도 확인했습니다.

특히 현재 OpenLDAP은 volume을 사용하고 있기 때문에 **LDAP volume을 삭제하면 기존 LDAP 데이터까지 없어질 수 있으므로 `docker compose down -v`는 주의**해야 합니다.

---

# 4. Keycloak Realm

Keycloak에서는 기존에 사용하던 realm:

```text
hdaic
```

를 사용하고 있습니다.

즉 구조는:

```text
Keycloak
└── hdaic
    ├── Clients
    │   └── usage-api
    │
    ├── Realm Roles
    │   ├── user
    │   └── ldap-admin
    │
    └── User Federation
        └── OpenLDAP
```

중요한 점은 **Keycloak Admin Console의 현재 realm이 `hdaic`인지 확인하는 것**입니다.

과거 로그인 문제에서:

```text
realmName="master"
clientId="security-admin-console"
error="user_not_found"
```

가 발생한 적이 있었는데, 이는 `hdaic` 사용자를 `master` realm에서 찾으려 했던 상황이었습니다.

---

# 5. Keycloak ↔ OpenLDAP User Federation

Keycloak에서 다음 메뉴를 사용했습니다.

```text
hdaic
  ↓
User Federation
  ↓
Add LDAP provider
```

개념적으로:

```text
Keycloak
   │
   │ LDAP
   ▼
ldap://openldap:389
   │
   ▼
dc=example,dc=org
   │
   └── ou=people
```

Docker Compose 네트워크 안에서는 Keycloak이 LDAP 서버를:

```text
ldap://openldap:389
```

로 접근하는 것이 핵심입니다.

Keycloak 공식 문서에서도 LDAP provider를 `User Federation → Add LDAP provider`에서 설정하도록 설명하고 있습니다. ([GitHub][1])

---

# 6. LDAP 사용자 속성 Mapping

이번 실습에서 특히 중요한 부분입니다.

LDAP:

```text
uid
cn
givenName
sn
mail
departmentNumber
userPassword
```

등의 속성을 Keycloak의 사용자 정보와 연결합니다.

개념적으로:

```text
OpenLDAP                  Keycloak

uid       ─────────────→  username
cn        ─────────────→  name
givenName ─────────────→  firstName
sn        ─────────────→  lastName
mail      ─────────────→  email
```

Keycloak LDAP mapper는 이러한 LDAP 속성과 Keycloak 사용자 속성을 연결하는 역할을 합니다. ([GitHub][1])

---

# 7. LDAP 사용자 `kim` 테스트

현재 확인한 Keycloak `/me` 결과는:

```json
{
  "user_id": "c03fb983-3419-44eb-867f-823d83804e52",
  "username": "kim",
  "email": "kim@example.com",
  "name": "Kim User",
  "roles": [
    "user"
  ]
}
```

즉 현재:

```text
kim
 ├─ username : kim
 ├─ email    : kim@example.com
 ├─ name     : Kim User
 └─ role     : user
```

까지는 정상적으로 애플리케이션에서 인식되고 있습니다.

다만 `ldap-admin` 역할은 없습니다.

따라서:

```javascript
isLdapAdmin()
```

결과가:

```text
false
```

가 되어 LDAP 관리자 UI가 숨겨지는 문제가 있었습니다.

---

# 8. `ldap-admin` 역할 추가

현재 UI에서는 다음 조건으로 LDAP 관리 화면을 표시합니다.

```javascript
const LDAP_ADMIN_ROLE = "ldap-admin";
```

그리고:

```javascript
roles.includes("ldap-admin")
```

이면 LDAP 관리 화면을 보여줍니다.

따라서 Keycloak에서 `kim`에게:

```text
hdaic
 └── Realm Roles
       └── ldap-admin
```

을 부여해야 합니다.

그러면 `/me`가:

```json
{
  "username": "kim",
  "roles": [
    "user",
    "ldap-admin"
  ]
}
```

처럼 되는 것이 목표입니다.

---

# 9. FastAPI의 LDAP 관리자 권한

중요한 점은 **UI에서만 권한을 검사하는 것이 아니라 FastAPI에서도 다시 검사한다는 것**입니다.

현재 개념은:

```python
async def require_ldap_admin(user=Depends(get_current_user)):
    roles = user.get("realm_access", {}).get("roles", [])

    if "ldap-admin" not in roles:
        raise HTTPException(
            status_code=403,
            detail="Administrator role required"
        )

    return user
```

즉:

```text
Browser
   │
   │ JWT
   ▼
FastAPI
   │
   ├─ JWT 검증
   │
   ├─ 사용자 확인
   │
   └─ ldap-admin 확인
          │
          ├─ YES → LDAP API 실행
          │
          └─ NO  → 403
```

구조입니다.

따라서 UI에서 버튼을 강제로 표시하는 것만으로는 LDAP 사용자 등록 권한이 생기지 않습니다.

---

# 10. LDAP 사용자 등록 기능

관리자 UI에 다음 기능을 추가했습니다.

```text
LDAP 사용자 관리
│
├── 사용자 ID
├── 이름
├── 성
├── 이메일
├── 부서
├── 초기 비밀번호
│
└── LDAP 사용자 등록
```

FastAPI:

```text
POST /admin/ldap/users
```

를 호출합니다.

그리고 LDAP 사용자 생성 시 최종적으로:

```text
FastAPI
   │
   ▼
OpenLDAP
   │
   └── ou=people,dc=example,dc=org
```

에 사용자를 생성하는 구조입니다.

---

# 11. `full_name` 문제 해결

사용자 등록 과정에서 다음 오류가 발생했습니다.

```text
Field required

loc:
["body", "full_name"]
```

UI에서 전송하는 데이터가:

```json
{
  "username": "yoonji",
  "first_name": "yoonji",
  "last_name": "kim",
  "email": "yoonji@test.com",
  "password": "test1234",
  "department": ""
}
```

였는데 FastAPI `LDAPUserCreate` 모델은:

```text
full_name
```

을 필수로 요구하고 있었습니다.

그래서 현재 UI에서는:

```javascript
const payload = {
    username: ...,
    full_name: `${firstName} ${lastName}`.trim(),
    first_name: ...,
    last_name: ...,
    email: ...,
    password: ...,
    department: ...
};
```

형태로 수정했습니다.

---

# 12. 테스트용 password 최소 길이

개발/실습 목적으로 password 최소 길이를 낮추는 작업도 진행했습니다.

예:

```python
password: str = Field(..., min_length=5)
```

또는 Pydantic 모델에서:

```python
class LDAPUserCreate(BaseModel):
    ...
    password: str = Field(..., min_length=5)
```

처럼 설정합니다.

실제 운영환경에서는 너무 짧은 비밀번호를 허용하지 않는 것이 좋습니다.

---

# 13. 사용자 등록 후 목록 조회

UI에는:

```text
등록 사용자 목록
       [목록 새로고침]
```

버튼을 추가했습니다.

API:

```text
GET /admin/ldap/users
```

을 호출하여 OpenLDAP의 사용자를 조회합니다.

UI에서는 LDAP 결과의:

```text
uid
cn
mail
departmentNumber
```

을 테이블 형태로 표시합니다.

최근에는 **사용자 등록 자체는 성공하지만 "목록 새로고침" 버튼이 동작하지 않는 문제**가 남아 있었고, 이를 수정하면서 UI 전체 스타일 개선 작업을 진행하려는 상태입니다.

---

# 14. 현재까지의 전체 흐름

지금까지 만든 것을 하나의 흐름으로 보면 상당히 좋은 실습 구조가 되었습니다.

```text
                ┌─────────────────┐
                │    Browser UI   │
                └────────┬────────┘
                         │
                         │ username/password
                         ▼
                ┌─────────────────┐
                │    Keycloak      │
                │   Realm hdaic    │
                └────────┬────────┘
                         │
                         │ LDAP Federation
                         ▼
                ┌─────────────────┐
                │    OpenLDAP      │
                │  LDAP Directory  │
                └─────────────────┘


로그인 성공 후

Browser
   │
   │ JWT
   ▼
FastAPI
   │
   ├── /me
   │
   ├── /usage
   │
   └── /admin/ldap/users
           │
           ▼
       OpenLDAP
```

그리고 기존 데이터 파이프라인까지 합치면:

```text
                         Authentication
                              │
                              ▼
                        ┌──────────┐
                        │ Keycloak │
                        └────┬─────┘
                             │
                       LDAP Federation
                             │
                             ▼
                        ┌──────────┐
                        │ OpenLDAP │
                        └──────────┘


                         JWT
                          │
                          ▼
Browser ──────────────→ FastAPI
                          │
                          │ usage event
                          ▼
                        Kafka
                          │
                          ▼
                        Spark
                          │
                          ▼
                       Iceberg
                          │
                          ▼
                         MinIO
```

즉 현재 프로젝트가 단순한 **Kafka/Iceberg 실습**에서 한 단계 발전해서,

> **Identity → Authorization → User Directory → API → Event Pipeline → Data Lake**

를 하나의 Docker 기반 실습 환경에서 연결하는 형태가 되었습니다.

---

## 15. 현재 상태 정리

| 영역                         | 상태               |
| -------------------------- | ---------------- |
| OpenLDAP Docker 설치         | ✅                |
| LDAP 데이터 영속 Volume         | ✅                |
| LDAP `dc=example,dc=org`   | ✅                |
| LDAP `ou=people`           | ✅                |
| Keycloak `hdaic` Realm     | ✅                |
| Keycloak ↔ LDAP Federation | ✅/설정 진행          |
| LDAP 사용자 `kim`             | ✅                |
| Keycloak 로그인               | ✅                |
| `/me` JWT 사용자 정보           | ✅                |
| `user` Realm Role          | ✅                |
| `ldap-admin` Role          | 🔧 현재 `kim`에는 없음 |
| FastAPI LDAP Admin 권한검사    | ✅                |
| FastAPI LDAP 사용자 생성        | ✅                |
| `full_name` 필드 문제          | ✅ 수정             |
| 테스트 password 최소 5자         | ✅                |
| LDAP 사용자 목록 조회             | ✅                |
| 목록 새로고침 버튼                 | 🔧 수정 필요         |
| LDAP UI 디자인 개선             | 🔧 진행 대상         |

그리고 Keycloak의 LDAP Federation은 단순히 LDAP을 인증에 사용하는 것뿐 아니라, **사용자 import/sync, attribute mapper, role/group mapper, LDAP password validation**까지 확장할 수 있습니다. ([GitHub][1])

### 다음 단계

현재 실습에서 가장 자연스러운 다음 단계는 **`ldap-admin` 역할까지 포함한 완성형 구조**를 만드는 것입니다.

```text
OpenLDAP
   │
   ├── 사용자 생성
   ├── 사용자 조회
   └── 사용자 속성
          │
          ▼
     Keycloak LDAP Federation
          │
          ├── username
          ├── email
          ├── name
          └── roles/groups
                 │
                 ▼
              JWT
                 │
                 ▼
              FastAPI
                 │
        ┌────────┴────────┐
        │                 │
     일반 사용자       ldap-admin
        │                 │
      /usage        /admin/ldap/users
        │                 │
        ▼                 ▼
      Kafka            OpenLDAP
        │
        ▼
      Spark
        │
        ▼
     Iceberg
        │
        ▼
      MinIO
```

이 구조까지 완성하면 **“Keycloak이 인증, OpenLDAP이 사용자 저장소, FastAPI가 API 권한 제어, Kafka/Spark/Iceberg가 데이터 파이프라인”**&#xC774;라는 매우 명확한 실습 아키텍처가 됩니다.

[1]: https://github.com/keycloak/keycloak/blob/main/docs/documentation/server_admin/topics/user-federation/ldap.adoc?utm_source=chatgpt.com "keycloak/docs/documentation/server_admin/topics/user-federation/ldap.adoc at main · keycloak/keycloak · GitHub"
