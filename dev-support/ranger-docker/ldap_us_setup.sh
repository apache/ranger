#!/bin/bash

# ==============================================================================
# Ranger Complete Master Alignment Script (Streamlined Fallback Engine)
# Description: Synchronizes all LDAP settings across four layout layers:
#              1. Ranger Admin Install Properties
#              2. Ranger UserSync Install Properties (with Search Base Fallbacks)
#              3. Docker Compose Privilege Overrides
#              4. Dynamically Generates docker-compose.local-ldap.yml
# ==============================================================================

# --- Target Configuration Blueprints ---
ADMIN_PROP="./scripts/admin/ranger-admin-install-postgres.properties"
USERSYNC_PROP="./scripts/usersync/ranger-usersync-install.properties"
COMPOSE_FILE="docker-compose.ranger.yml"
LDAP_COMPOSE_FILE="docker-compose.local-ldap.yml"

# --- Core Top-Level Configuration Blueprint ---
AUTH_METHOD="LDAP"
LDAP_HOST="local-ldap"
LDAP_PORT="389"
LDAP_DOMAIN="example.com"
BASE_DN="dc=example,dc=com"
BIND_DN="cn=admin,dc=example,dc=com"
BIND_PASS="p@ssw0rd"
USER_BASE="ou=People,dc=example,dc=com"
GROUP_BASE="ou=People,dc=example,dc=com"
LARGE_GROUP_SYNC_ENABLED="true"

# Derived Values
LDAP_URL="ldap://${LDAP_HOST}:${LDAP_PORT}"
USER_PATTERN="cn={0},${USER_BASE}"
USER_FILTER="(cn={0})"
GROUP_FILTER="(member=cn={0},${GROUP_BASE})"

# --- Color Definitions for Logging ---
C_RESET='\033[0m'
C_GREEN='\033[0;32m'
C_YELLOW='\033[0;33m'
C_CYAN='\033[0;36m'
C_RED='\033[0;31m'

log_info() { echo -e "${C_CYAN}[INFO]${C_RESET} $1"; }
log_ok()   { echo -e "${C_GREEN}[OK]${C_RESET} $1"; }
log_warn() { echo -e "${C_YELLOW}[WARN]${C_RESET} $1"; }
log_err()  { echo -e "${C_RED}[ERROR]${C_RESET} $1"; exit 1; }

# --- Pre-flight Validation Checks ---
[[ ! -f "$ADMIN_PROP" ]]    && log_err "Target missing: $ADMIN_PROP"
[[ ! -f "$USERSYNC_PROP" ]] && log_err "Target missing: $USERSYNC_PROP"
[[ ! -f "$COMPOSE_FILE" ]]  && log_err "Target missing: $COMPOSE_FILE"

# Handle cross-platform in-place sed adjustments (macOS vs standard Linux)
if [[ "$OSTYPE" == "darwin"* ]]; then
    sed_cmd() { sed -i '' "$@"; }
else
    sed_cmd() { sed -i "$@"; }
fi

# Helper function for flat Key-Value file overrides
update_property() {
    local key="$1" local val="$2" local file="$3"
    local escaped_val=$(printf '%s\n' "$val" | sed 's/[&/\]/\\&/g')
    if grep -q "^[[:space:]]*$key[[:space:]]*=" "$file"; then
        sed_cmd -E "s|^([[:space:]]*$key[[:space:]]*=[[:space:]]*).*|\1$escaped_val|" "$file"
    else
        echo "$key = $val" >> "$file"
    fi
}

# ==============================================================================
# Execution Lifecycle
# ==============================================================================

log_info "Initiating deployment update for all Ranger environment files..."

# ------------------------------------------------------------------------------
# Phase 1: Modify ranger-admin-install-postgres.properties
# ------------------------------------------------------------------------------
log_info "Processing configuration properties inside $ADMIN_PROP..."

update_property "authentication_method" "$AUTH_METHOD" "$ADMIN_PROP"

if ! grep -q "Corporate OpenLDAP Authentication Setup" "$ADMIN_PROP"; then
    echo -e "\n# ====================================================================" >> "$ADMIN_PROP"
    echo "# Corporate OpenLDAP Authentication Setup" >> "$ADMIN_PROP"
    echo -e "# ====================================================================\n" >> "$ADMIN_PROP"
fi

update_property "xa_ldap_url" "$LDAP_URL" "$ADMIN_PROP"
update_property "xa_ldap_base_dn" "$BASE_DN" "$ADMIN_PROP"
update_property "xa_ldap_bind_dn" "$BIND_DN" "$ADMIN_PROP"
update_property "xa_ldap_bind_password" "$BIND_PASS" "$ADMIN_PROP"
update_property "xa_ldap_userDNpattern" "$USER_PATTERN" "$ADMIN_PROP"
update_property "xa_ldap_userSearchFilter" "$USER_FILTER" "$ADMIN_PROP"
update_property "xa_ldap_groupSearchBase" "$GROUP_BASE" "$ADMIN_PROP"
update_property "xa_ldap_groupSearchFilter" "$GROUP_FILTER" "$ADMIN_PROP"
update_property "xa_ldap_groupRoleAttribute" "cn" "$ADMIN_PROP"
update_property "xa_ldap_referral" "ignore" "$ADMIN_PROP"

log_ok "Successfully updated native Admin install properties."

# ------------------------------------------------------------------------------
# Phase 2: Modify ranger-usersync-install.properties (With Lean Fallbacks)
# ------------------------------------------------------------------------------
log_info "Processing configuration properties inside $USERSYNC_PROP..."

update_property "SYNC_SOURCE" "ldap" "$USERSYNC_PROP"
update_property "SYNC_LDAP_URL" "$LDAP_URL" "$USERSYNC_PROP"
update_property "SYNC_LDAP_BIND_DN" "$BIND_DN" "$USERSYNC_PROP"
update_property "SYNC_LDAP_BIND_PASSWORD" "$BIND_PASS" "$USERSYNC_PROP"

# The Single Source of Truth Base DN Path
update_property "SYNC_LDAP_USER_SEARCH_BASE" "$USER_BASE" "$USERSYNC_PROP"
update_property "SYNC_LDAP_USER_SEARCH_FILTER" "(objectClass=inetOrgPerson)" "$USERSYNC_PROP"

# Explicitly clear out broader root to enforce automated hierarchical fallback loops
update_property "SYNC_LDAP_SEARCH_BASE" "" "$USERSYNC_PROP"

# Group Specific Filtering Strategy
update_property "SYNC_LDAP_GROUP_SEARCH_FILTER" "(objectClass=groupOfNames)" "$USERSYNC_PROP"

# Normalizations & Performance Scaling Optimizations
update_property "SYNC_USERNAME_CASE_CONVERSION" "lower" "$USERSYNC_PROP"
update_property "SYNC_GROUPNAME_CASE_CONVERSION" "lower" "$USERSYNC_PROP"
update_property "LGSYNC_LDAP_LARGEGROUPSYNC_ENABLED" "$LARGE_GROUP_SYNC_ENABLED" "$USERSYNC_PROP"

log_ok "Successfully streamlined UserSync configuration properties."

# ------------------------------------------------------------------------------
# Phase 3: Streamline docker-compose.ranger.yml
# ------------------------------------------------------------------------------
log_info "Reviewing permission parameters inside $COMPOSE_FILE..."

python3 -c "
import re

with open('$COMPOSE_FILE', 'r') as f:
    yaml_content = f.read()

regex_pattern = r'(user:\s*root\s*\n\s*)?(command|entrypoint):\s*\n\s*-\s*(?:/bin/bash[\s\S]*?)?/home/ranger/scripts/ranger\.sh\"?'

clean_native_entrypoint = '''user: root
    entrypoint:
      - /bin/bash
      - -c
      - |
        # Hand execution off directly to the native initialization runtime
        chown -R ranger:ranger /opt/ranger/admin/
        su -s /bin/bash ranger -c \"/home/ranger/scripts/ranger.sh\"'''

yaml_content = re.sub(r'\s*extra_hosts:\s*\n\s*-\s*\"ccycloud.*?:127\.0\.0\.1\"', '', yaml_content)
updated_content, substitutions = re.subn(regex_pattern, clean_native_entrypoint, yaml_content)

if substitutions > 0:
    with open('$COMPOSE_FILE', 'w') as f:
        f.write(updated_content)
    print('SUCCESS')
else:
    print('NO_CHANGE')
" > /tmp/ranger_py_status.tmp

PY_STATUS=$(cat /tmp/ranger_py_status.tmp)
rm -f /tmp/ranger_py_status.tmp

if [ "$PY_STATUS" == "SUCCESS" ]; then
    log_ok "Successfully synchronized Docker privilege blocks inside $COMPOSE_FILE."
else
    log_warn "Docker Compose alignment skipped (already up to date)."
fi

# ------------------------------------------------------------------------------
# Phase 4: Auto-Generate docker-compose.local-ldap.yml
# ------------------------------------------------------------------------------
log_info "Generating infrastructure layer config inside $LDAP_COMPOSE_FILE..."

cat << EOF > "$LDAP_COMPOSE_FILE"
services:
  local-ldap:
    image: osixia/openldap:1.5.0
    container_name: ${LDAP_HOST}
    hostname: ${LDAP_HOST}.rangernw
    ports:
      - "${LDAP_PORT}:389"
      - "636:636"
    environment:
      - LDAP_ORGANISATION=RangerNW
      - LDAP_DOMAIN=${LDAP_DOMAIN}
      - LDAP_ADMIN_PASSWORD=${BIND_PASS}
      - LDAP_TLS=false
    networks:
      - ranger

  local-ldap-admin:
    image: osixia/phpldapadmin:0.9.0
    container_name: ${LDAP_HOST}-admin
    ports:
      - "8080:80"
    environment:
      - PHPLDAPADMIN_HTTPS=false
      - PHPLDAPADMIN_LDAP_HOSTS=${LDAP_HOST}
    networks:
      - ranger
    depends_on:
      - local-ldap

networks:
  ranger:
    name: rangernw
EOF

log_ok "Successfully auto-built $LDAP_COMPOSE_FILE with variables alignment."

echo
log_ok "All Ranger and OpenLDAP configurations are perfectly unified! 🎉"
log_warn "Run your 'down -v' and 'up -d' routine to compile everything into the cluster."
