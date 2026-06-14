<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at
  http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->
# Audit ingestor: plugin 403 (service allowlist)

When a Ranger plugin uses the **audit-server** destination (`xasecure.audit.destination.auditserver`), it POSTs audit batches to audit-ingestor at `/audit/access?serviceName=<repo>`. Failures often look like authentication problems but are usually **authorization (403)** — Kerberos/SPNEGO succeeded; the mapped service user is not on the ingestor allowlist for that Policy Manager repo.

Related: [dev-support/ranger-docker/README.md](../dev-support/ranger-docker/README.md), shipped defaults in `audit-ingestor/src/main/resources/conf/ranger-audit-ingestor-site.xml`.

---

## Quick read: 401 vs 403

| HTTP | Meaning | Typical cause |
|------|---------|---------------|
| **401** | Authentication failed | Missing/invalid Kerberos ticket, wrong SPNEGO, clock skew, wrong ingestor URL |
| **403** | Authentication OK, **authorization failed** | Caller authenticated but short username not in `ranger.audit.ingestor.service.<repo>.allowed.users` |

If ingestor logs show `Unauthorized user: user=<name> ... service=<repo>`, Kerberos worked — fix the allowlist or `auth_to_local`, not the keytab.

---

## What's happening

```text
Plugin (e.g. KMS)                    audit-ingestor
     |                                      |
     |  POST /audit/access?serviceName=dev_kms
     |  Authorization: Negotiate (SPNEGO)   |
     |------------------------------------->|
     |                                      | 1. Authenticate Kerberos principal
     |                                      |    e.g. rangerkms/host@REALM
     |                                      | 2. auth_to_local → short name "rangerkms"
     |                                      | 3. isAllowedServiceUser(dev_kms, rangerkms)
     |                                      |    checks allowed.users for dev_kms
     |                                      | 4. Not listed → 403 Forbidden
     |<-------------------------------------|
     |  {"message":"User is not authorized to send audit events",...}
```

Implementation: `AuditREST.java` → `isAllowedServiceUser()`. Config keys:

```properties
ranger.audit.ingestor.service.<repoName>.allowed.users=<comma-separated short names>
ranger.audit.ingestor.auth.to.local=<Kerberos RULE lines>
```

`<repoName>` must match the Policy Manager **service name** (e.g. `dev_kms`, `dev_hdfs`), not the plugin type alone.

---

## Example: Ranger KMS (403)

**Plugin log:**

```text
ERROR o.a.r.a.d.RangerAuditServerDestination - Failed to send audit batch.
HTTP status: 403, Response: {"message":"User is not authorized to send audit events",...}
ERROR o.a.r.a.d.RangerAuditServerDestination - Failed to send batch of 1 events
```

**audit-ingestor log:**

```text
ERROR - Unauthorized user: user=rangerkms is authorized report audit logs for service=dev_kms.
Rejecting audit request.
```

**Interpretation:** SPNEGO to ingestor works. `rangerkms` is not allowed for repo `dev_kms` (missing property, wrong short name, or stale runtime config).

**Fix:**

```xml
<property>
    <name>ranger.audit.ingestor.service.dev_kms.allowed.users</name>
    <value>rangerkms</value>
</property>
```

Ensure `auth_to_local` includes `rangerkms/*` → `rangerkms`, restart ingestor, confirm the **running** pod/container loads the updated site XML.

---

## Root causes (side-by-side)

| # | Symptom | Likely cause | Fix |
|---|---------|--------------|-----|
| 1 | 403, ingestor log names user + repo | No `allowed.users` for that repo | Add `ranger.audit.ingestor.service.<repo>.allowed.users` |
| 2 | 403, user name looks wrong (e.g. `kms` vs `rangerkms`) | Allowlist short name ≠ `auth_to_local` output | Align with `policy.download.auth.users` in Ranger Admin |
| 3 | 403 for Ozone OM/SCM/DN only | Allowlist has `ozone` but principal maps to `om`/`scm`/`dn` | Use `ozone,om,scm,dn` for `dev_ozone` |
| 4 | 403 after editing source XML only | Ingestor still using old classpath copy | Patch `conf/` **and** `WEB-INF/classes/conf/`, or rebuild/redeploy image |
| 5 | 403, repo name unexpected | Plugin `serviceName` ≠ Policy Manager service name | Match repo name in plugin audit config to Ranger Admin service |
| 6 | 401 instead of 403 | Kerberos/SPNEGO failure | Keytab, principal, `_HOST`, ingestor URL/FQDN, KDC — not allowlist |

---

## How to fix

### 1. Identify repo and short username

From ingestor error line: `user=<shortname> ... service=<repo>`.

Or from Ranger Admin → service → **Configs** → `policy.download.auth.users` (ingestor allowlist should match).

### 2. Add or update allowlist

In `ranger-audit-ingestor-site.xml` (or deployment overlay):

```xml
<property>
    <name>ranger.audit.ingestor.service.dev_<service>.allowed.users</name>
    <value><short names, comma-separated></value>
</property>
```

### 3. Verify `auth_to_local`

Property: `ranger.audit.ingestor.auth.to_local`. Service-specific `RULE:` lines must run **before** the generic `user@REALM` fallback so `service/host@REALM` maps to the same short names as the allowlist.

### 4. Restart audit-ingestor

Allowlist is loaded at startup. After change, restart the ingestor pod/container.

### 5. Docker dev stack extras

| Step | Action |
|------|--------|
| Policy Manager repos | `docker exec ranger python3 /home/ranger/scripts/create-ranger-services.py` (idempotent) |
| New plugin / new repo | Add allowlist entry + `auth_to_local` rule + create service in Admin |

---

## Shipped allowlist reference (Docker dev repos)

Align each row with `policy.download.auth.users` for that service in Policy Manager.

| Policy Manager repo | `allowed.users` (short names) |
|---------------------|-------------------------------|
| `dev_hdfs` | `hdfs` |
| `dev_yarn` | `yarn` |
| `dev_hive` | `hive` |
| `dev_hbase` | `hbase` |
| `dev_kafka` | `kafka` |
| `dev_knox` | `knox` |
| `dev_kms` | `rangerkms` |
| `dev_trino` | `trino` |
| `dev_ozone` | `ozone,om,scm,dn` |
| `dev_solr` | `solr` |
| `dev_atlas` | `atlas` |
| `dev_kudu` | `kudu` |
| `dev_nifi` | `nifi` |

For a **new** repo, add both the property and a matching `auth_to_local` rule; there is no wildcard allowlist.

---

## Verification

1. Trigger an access event on the plugin (e.g. KMS operation).
2. Plugin log: no `HTTP status: 403` from `RangerAuditServerDestination`.
3. Ingestor log: no `Unauthorized user` for that repo; debug may show `isAllowedServiceUser(...): ret=true`.
4. Downstream (optional): audit count increases in Kafka/Solr/Admin UI.

---

## Product vs Docker scope

| Scope | Problem | Owner / fix |
|-------|---------|-------------|
| **Product** | Fresh install missing allowlist for enabled plugins | Ship complete defaults in `ranger-audit-ingestor-site.xml`; document in install guide |
| **Docker / ops** | Stack built before allowlist was added; stale WEB-INF copy | Patch/redeploy ingestor; run `create-ranger-services.py` |

Same 403 mechanism in both cases; difference is whether defaults are shipped vs patched at deploy time.

---

## Code references

| Piece | Location |
|-------|----------|
| Authorization check | `audit-ingestor/.../AuditREST.java` — `isAllowedServiceUser()` |
| Property prefix | `ranger.audit.ingestor.service.` + `<repo>` + `.allowed.users` |
| Default config | `audit-ingestor/src/main/resources/conf/ranger-audit-ingestor-site.xml` |
| Plugin client | `agents-audit/.../RangerAuditServerDestination.java` |
