#!/bin/sh
set -eu

apk add --no-cache curl jq >/dev/null

KC="${KEYCLOAK_URL%/}"
REALM="${KEYCLOAK_REALM:-hdaic}"
PROVIDER_NAME="openldap-user-federation"

echo "[keycloak-ldap-init] Waiting for Keycloak admin API..."
TOKEN=""
i=0
while [ "$i" -lt 90 ]; do
  TOKEN="$(curl -fsS -X POST "$KC/realms/master/protocol/openid-connect/token" \
    -H 'Content-Type: application/x-www-form-urlencoded' \
    --data-urlencode 'grant_type=password' \
    --data-urlencode "username=$KEYCLOAK_ADMIN_USERNAME" \
    --data-urlencode "password=$KEYCLOAK_ADMIN_PASSWORD" \
    --data-urlencode 'client_id=admin-cli' 2>/dev/null | jq -r '.access_token // empty' || true)"
  if [ -n "$TOKEN" ]; then
    break
  fi
  i=$((i + 1))
  sleep 3
done

if [ -z "$TOKEN" ]; then
  echo "[keycloak-ldap-init] ERROR: Could not obtain Keycloak admin token." >&2
  exit 1
fi

AUTH="Authorization: Bearer $TOKEN"

echo "[keycloak-ldap-init] Waiting for realm '$REALM'..."
REALM_ID=""
i=0
while [ "$i" -lt 60 ]; do
  REALM_ID="$(curl -fsS -H "$AUTH" "$KC/admin/realms/$REALM" 2>/dev/null | jq -r '.id // empty' || true)"
  [ -n "$REALM_ID" ] && break
  i=$((i + 1))
  sleep 3
done

if [ -z "$REALM_ID" ]; then
  echo "[keycloak-ldap-init] ERROR: Realm '$REALM' was not found. Check realm import." >&2
  exit 1
fi

PAYLOAD="$(jq -n \
  --arg parentId "$REALM_ID" \
  --arg bindDn "$LDAP_BIND_DN" \
  --arg bindPassword "$LDAP_BIND_PASSWORD" \
  --arg usersDn "$LDAP_USERS_DN" \
  '{
    name: "openldap-user-federation",
    providerId: "ldap",
    providerType: "org.keycloak.storage.UserStorageProvider",
    parentId: $parentId,
    config: {
      enabled: ["true"],
      priority: ["0"],
      vendor: ["other"],
      connectionUrl: ["ldap://openldap:389"],
      usersDn: [$usersDn],
      authType: ["simple"],
      bindDn: [$bindDn],
      bindCredential: [$bindPassword],
      usernameLDAPAttribute: ["uid"],
      rdnLDAPAttribute: ["uid"],
      uuidLDAPAttribute: ["entryUUID"],
      userObjectClasses: ["inetOrgPerson, organizationalPerson, person"],
      searchScope: ["1"],
      editMode: ["READ_ONLY"],
      importEnabled: ["true"],
      syncRegistrations: ["false"],
      connectionPooling: ["true"],
      pagination: ["true"]
    }
  }')"

COMPONENTS="$(curl -fsS -G -H "$AUTH" \
  --data-urlencode 'type=org.keycloak.storage.UserStorageProvider' \
  "$KC/admin/realms/$REALM/components")"

EXISTING_ID="$(printf '%s' "$COMPONENTS" | jq -r --arg name "$PROVIDER_NAME" '.[] | select(.name == $name) | .id' | head -n 1)"

if [ -n "$EXISTING_ID" ]; then
  echo "[keycloak-ldap-init] Updating existing LDAP provider ($EXISTING_ID)..."
  HTTP_CODE="$(curl -sS -o /tmp/keycloak-response.txt -w '%{http_code}' \
    -X PUT "$KC/admin/realms/$REALM/components/$EXISTING_ID" \
    -H "$AUTH" -H 'Content-Type: application/json' --data "$PAYLOAD")"
else
  echo "[keycloak-ldap-init] Creating LDAP provider..."
  HTTP_CODE="$(curl -sS -o /tmp/keycloak-response.txt -w '%{http_code}' \
    -X POST "$KC/admin/realms/$REALM/components" \
    -H "$AUTH" -H 'Content-Type: application/json' --data "$PAYLOAD")"
fi

case "$HTTP_CODE" in
  200|201|204)
    echo "[keycloak-ldap-init] LDAP provider is configured successfully."
    ;;
  *)
    echo "[keycloak-ldap-init] ERROR: Keycloak returned HTTP $HTTP_CODE" >&2
    cat /tmp/keycloak-response.txt >&2
    exit 1
    ;;
esac

echo "[keycloak-ldap-init] Triggering LDAP user synchronization..."
PROVIDER_ID=""
COMPONENTS="$(curl -fsS -G -H "$AUTH" \
  --data-urlencode 'type=org.keycloak.storage.UserStorageProvider' \
  "$KC/admin/realms/$REALM/components")"
PROVIDER_ID="$(printf '%s' "$COMPONENTS" | jq -r --arg name "$PROVIDER_NAME" '.[] | select(.name == $name) | .id' | head -n 1)"

if [ -n "$PROVIDER_ID" ]; then
  curl -fsS -X POST \
    "$KC/admin/realms/$REALM/user-storage/$PROVIDER_ID/sync?action=triggerFullSync" \
    -H "$AUTH" >/dev/null
  echo "[keycloak-ldap-init] Full LDAP user sync completed."
fi
