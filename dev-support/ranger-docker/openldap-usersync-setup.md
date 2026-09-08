# Containerized Apache Ranger & OpenLDAP Native Integration

This blueprint covers the implementation, property configurations, and lifecycle operations required to run an integrated Apache Ranger stack synchronized natively with a local OpenLDAP server on Docker.

## 1. Run the Prerequisite Setup Script

Execute the setup script (`ldap_us_setup.sh`) to automatically configure the required integration settings for the Docker environment. This script acts as a **Master Alignment Script** that seamlessly synchronizes LDAP settings across four key layers:

- **Ranger Admin Properties:** Injects the core Corporate OpenLDAP authentication variables into `ranger-admin-install-postgres.properties`.
- **Ranger UserSync Properties:** Configures source targets, lean search base fallbacks, and group synchronization optimizations inside `ranger-usersync-install.properties`.
- **Docker Compose Overrides:** Safely aligns container privilege blocks (`user: root`) and native initialization runtimes within `docker-compose.ranger.yml`.
- **Infrastructure Generation:** Dynamically auto-generates the `docker-compose.local-ldap.yml` file with matching environment variables (domains, passwords, and mapped ports).

> **IMPORTANT:** Running this script **fully automates the manual configurations described in Step 2 and Step 4**. If you execute this script, please **do not** manually create the Compose file or edit the properties mentioned below—doing both will cause conflicts. Sections 2 and 4 are provided strictly as a reference matrix. After running the script, you can jump straight to Step 3 (LDIF seed) and Step 5 (Deployment Lifecycle).

```bash
./ldap_us_setup.sh
```

---

## 2. Infrastructure Layer Setup (Reference)

Deploy this file (directory: `ranger/dev-support/ranger-docker`) to spin up your ARM64-compatible local directory service daemon and web admin console. It is configured to join the internal network stack natively.

> **Note:** If you ran `ldap_us_setup.sh` in Step 1, this file is generated automatically. The snippet below is for reference only.

```yaml
services:
  local-ldap:
    image: osixia/openldap:1.5.0
    container_name: local-ldap
    hostname: local-ldap.rangernw
    ports:
      - "389:389"
      - "636:636"
    environment:
      - LDAP_ORGANISATION=RangerNW
      - LDAP_DOMAIN=example.com
      - LDAP_ADMIN_PASSWORD=p@ssw0rd
      - LDAP_TLS=false
    networks:
      - ranger

  local-ldap-admin:
    image: osixia/phpldapadmin:0.9.0
    container_name: local-ldap-admin
    ports:
      - "8080:80"
    environment:
      - PHPLDAPADMIN_HTTPS=false
      - PHPLDAPADMIN_LDAP_HOSTS=local-ldap
    networks:
      - ranger
    depends_on:
      - local-ldap

networks:
  ranger:
    name: rangernw
```

---

## 3. Directory Information Tree Seed (`ldapusergroup.ldif`)

Create this data file to map out your primary directory structures. It creates the required `ou=People` structural organizational unit, seeds the user `john`, and provisions the `engineering` group with `john` assigned as an active member.

```ldif
# 1. Base Structural Container
dn: ou=People,dc=example,dc=com
objectClass: organizationalUnit
ou: People

# 2. User Profile
dn: cn=john,ou=People,dc=example,dc=com
cn: john
sn: doe
objectClass: inetOrgPerson
userPassword: password123
uid: john

# 3. Group Profile
dn: cn=engineering,ou=People,dc=example,dc=com
cn: engineering
objectClass: groupOfNames
member: cn=john,ou=People,dc=example,dc=com
```

---

## 4. Configuration Profiles Reference Matrix

Once fully synchronized via automation, your configuration files must maintain the following parameters to execute native LDAP mapping and container privilege coordination.

> **Note:** If you ran `ldap_us_setup.sh` in Step 1, these settings are applied automatically. The values below are for reference only.

### 4.A Docker Compose State (`docker-compose.ranger.yml` Snippet)

The `ranger:` block utilizes `user: root` to safely bypass folder ownership permission locks while initializing custom execution scripts:

```yaml
    environment:
      - RANGER_VERSION
      - RANGER_DB_TYPE
      - KERBEROS_ENABLED
      - DEBUG_ADMIN=${DEBUG_ADMIN:-false}
      - JAVA_OPTS
    user: root
    entrypoint:
      - /bin/bash
      - -c
      - |
        # Hand execution off directly to the native initialization runtime
        chown -R ranger:ranger /opt/ranger/admin/
        su -s /bin/bash ranger -c "/home/ranger/scripts/ranger.sh"
```

### 4.B Ranger Admin Native Properties

File: `ranger/dev-support/scripts/admin/ranger-admin-install-postgres.properties`

```properties
authentication_method = LDAP

xa_ldap_url = ldap://local-ldap:389
xa_ldap_base_dn = dc=example,dc=com
xa_ldap_bind_dn = cn=admin,dc=example,dc=com
xa_ldap_bind_password = p@ssw0rd
xa_ldap_userDNpattern = cn={0},ou=People,dc=example,dc=com
xa_ldap_userSearchFilter = (cn={0})
xa_ldap_groupSearchBase = ou=People,dc=example,dc=com
xa_ldap_groupSearchFilter = (member=cn={0},ou=People,dc=example,dc=com)
xa_ldap_groupRoleAttribute = cn
xa_ldap_referral = ignore
```

### 4.C Ranger UserSync Native Properties

File: `ranger/dev-support/scripts/usersync/ranger-usersync-install.properties`

```properties
SYNC_SOURCE = ldap
SYNC_LDAP_URL = ldap://local-ldap:389
SYNC_LDAP_BIND_DN = cn=admin,dc=example,dc=com
SYNC_LDAP_BIND_PASSWORD = p@ssw0rd

SYNC_LDAP_USER_SEARCH_BASE = ou=People,dc=example,dc=com
SYNC_LDAP_USER_SEARCH_FILTER = (objectClass=inetOrgPerson)

SYNC_LDAP_GROUP_SEARCH_BASE = ou=People,dc=example,dc=com
SYNC_LDAP_GROUP_SEARCH_FILTER = (objectClass=groupOfNames)

SYNC_USERNAME_CASE_CONVERSION = lower
SYNC_GROUPNAME_CASE_CONVERSION = lower
LGSYNC_LDAP_LARGEGROUPSYNC_ENABLED = true
```

---

## 5. Deployment Lifecycle Workflow

To prevent synchronization timeout failures, follow this strict command execution sequence from the `ranger/dev-support/ranger-docker` directory:

```bash
# Step 1: Tear down existing infrastructure allocations and clear dynamic volumes
docker compose -f docker-compose.ranger.yml -f docker-compose.ranger-usersync.yml -f docker-compose.ranger-tagsync.yml -f docker-compose.ranger-pdp.yml -f docker-compose.ranger-kms.yml -f docker-compose.local-ldap.yml down -v

# Step 2: Boot the fresh container environment ecosystem clusters simultaneously
docker compose -f docker-compose.ranger.yml -f docker-compose.ranger-usersync.yml -f docker-compose.ranger-tagsync.yml -f docker-compose.ranger-pdp.yml -f docker-compose.ranger-kms.yml -f docker-compose.local-ldap.yml up -d

# Step 3: Inject directory mappings BEFORE UserSync executes initial lookups
sleep 10
docker exec -i local-ldap ldapadd -c -x -D "cn=admin,dc=example,dc=com" -w p@ssw0rd < ldapusergroup.ldif

# Step 4: Allow Admin tables to finish building, then cycle UserSync to force discovery
sleep 60
docker restart ranger-usersync
```

---

## 6. Verification Diagnostics

Execute this log tailing command to track validation states:

```bash
docker compose -f docker-compose.ranger.yml -f docker-compose.ranger-usersync.yml -f docker-compose.ranger-tagsync.yml -f docker-compose.ranger-pdp.yml -f docker-compose.ranger-kms.yml -f docker-compose.local-ldap.yml exec ranger-usersync sh -c "tail -f /var/log/ranger/usersync/*.log"
```

### Successful Sync Signatures

Look for the following log entries to confirm a healthy synchronization:

```text
INFO  - initializing source: org.apache.ranger.ldapusersync.process.LdapUserGroupBuilder
INFO  - LdapUserGroupBuilder.getGroups() completed with group count: 1
INFO  - LdapUserGroupBuilder.getUsers() completed with user count: 1
INFO  - PolicyMgrUserGroupBuilder - API returned: 200, No. of users uploaded to ranger admin = 1
```

---

## Restoring Unix-Based User Synchronization

To restore Unix-based user synchronization, remove the configuration settings listed above.
