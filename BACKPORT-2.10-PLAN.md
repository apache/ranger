# Apache Ranger 2.10 — Backport Plan (Audit Server + SPIFFE + SQL)

**Target branch:** `ranger-2.10`  
**Source:** `origin/master` (cherry-picks only — no merge)  
**Working clone:** `/Users/ramk/rangerRelease29/ranger`  
**Backport branch:** `backport-audit-spiffe-2.10`  
**Parent Jira:** _TBD (create before PR-1)_

## Goals

Bring to 2.10 from master:

1. **Ranger Audit Server** (ingestor + dispatchers + plugin destination + docker)
2. **SPIFFE / header-based authentication** (extends RANGER-5499 already on 2.10)
3. **SQL fixes** (MariaDB grant, audit DB patch 078)

## Out of scope (do not cherry-pick)

- **GDS (Governed Data Sharing) — not needed on 2.10.** On conflict resolution, always drop GDS imports, methods, and service-def wiring. If `--theirs` pulls GDS into `AssetMgr`, `RangerBizUtil`, `RangerBasePlugin`, or `RangerDefaultAuditHandler`, restore those files from `ranger-2.10` and re-apply only audit/SPIFFE hunks.
- GDS features (general)
- JDK 17 / master-only pom churn
- Full RANGER-4076 Jersey 2 migration (adapt audit commits to 2.10 Jersey 1.x instead)
- TagSync-only fixes (5658, 5656)
- Ozone JDK17 runner commits (`7685006a`, `96282864`)
- Unrelated Ozone upgrade fixes (5748) unless CI blocks

## Already on ranger-2.10 (skip)

| SHA | Jira | Note |
|-----|------|------|
| `0aad6958e` | RANGER-5718 | gson on PDP classpath |
| backports | RANGER-5712, 5709, 5710 | docker/KMS/audit store |
| lineage | RANGER-5499, RANGER-5617 | header auth base + PDP config |

## Jersey dependency note

Master order: `RANGER-5482` → `RANGER-4076` (Jersey 2) → `RANGER-5520` → …

**2.10 strategy (Option A):** Cherry-pick audit commits; on conflict, keep **com.sun.jersey** / 2.10 pom conventions. Do not pull full 4076.

---

## Wave 0 — Branch setup

- [ ] `git checkout ranger-2.10 && git pull origin ranger-2.10`
- [ ] `git fetch origin master`
- [ ] `git checkout -b backport-audit-spiffe-2.10`
- [ ] Create parent Jira + subtasks per wave

**Gate:** clean working tree, on latest `ranger-2.10`.

---

## Wave 1 — Audit Server foundation

| # | Status | SHA | Jira | Summary |
|---|--------|-----|------|---------|
| 1.1 | ✅ | `89ce14a26` | RANGER-5482 | Create Audit Server + dest-auditserver |
| 1.2 | ✅ | `3fd46dbe` | RANGER-5520 | Refactor ingestor/dispatcher |
| 1.3 | ✅ | `e3ab2b33` | RANGER-5613 | Audit Server Dockerfile dedup |
| 1.4 | ⏭️ | `d000d3e7e` | RANGER-5611 | TLS 1.3 — **deferred** (wide blast radius, not audit-only) |
| 1.5 | ✅ | `a23c30c6` | RANGER-4676/5615 | OpenSearch dispatcher |
| 1.6 | ✅ | `7017225e6` | RANGER-5654 | Solr Kerberos TGT relogin |
| 1.7 | ✅ | `2ad565fe6` | RANGER-5720 | DB patch 078 — x_audit_config |

**Cherry-pick:**

```bash
git cherry-pick -x 89ce14a26
# ... repeat per row; resolve conflicts keeping 2.10 Jersey/pom style
```

**Gate:**

```bash
mvn clean install -pl audit-server,agents-audit/dest-auditserver -am -DskipTests
# Apply patch 078 on Postgres/MySQL test DB
```

**PR:** `backport-audit-server-2.10` → `ranger-2.10` (Wave 1 only, or combined with Wave 2)

---

## Wave 2 — Plugin → audit-server wiring

| # | Status | SHA | Jira | Summary |
|---|--------|-----|------|---------|
| 2.1 | ✅ | `d2cd9ea7` | RANGER-5483 | Audit-server as plugin destination |
| 2.2 | ✅ | `e3babee0` | RANGER-5632 | Packaging: audit-server only destination |
| 2.3 | ✅ | `4a061759` | RANGER-5633 | Kafka producer/consumer tuning |
| 2.4 | ✅ | `10bda4d1` | RANGER-5642/5644 | Jersey client JARs in Kafka/HBase |
| 2.5 | ✅ | `33e7b3a3` | RANGER-5650 | KMS audit client JARs |
| 2.6 | ✅ | `6bf19137` | RANGER-5642 | Kafka duplicate Jersey exclude |
| 2.7 | ✅ | `9272baf0` | RANGER-5646 | Hive duplicate JAR exclude |
| 2.8 | ✅ | `6d9b1b2c` | RANGER-5661 | Kafka 3.9 classloading |
| 2.9 | ✅ | `abe67188` | RANGER-5660 | YARN packaging |
| 2.10 | ✅ | `09b53156` | RANGER-5640 | Ozone audit-server JARs |
| 2.11 | ✅ | `137a5dd4` | RANGER-5637 | Knox/Ozone docker CI |
| 2.12 | ✅ | `9ab006932` | RANGER-5645 | Ingestor service-user allowlist |
| 2.13 | ✅ | `ce93068d` | RANGER-5643 | Solr Kerberos docker |

**Gate:** Docker plugin smoke — audits reach audit-ingestor.

---

## Wave 3 — Docker audit stack

| # | Status | SHA | Jira | Summary |
|---|--------|-----|------|---------|
| 3.1 | ✅ | `d4a0759a4` | RANGER-5680 | Compose restructure + OpenSearch default |
| 3.2 | ✅ | `0249cc1d` | RANGER-5679 | OpenSearch docs, decouple Solr |

**Skip:** `7685006a`, `96282864` (JDK17 Ozone runner).

**Gate:** Full docker stack — admin + plugins + audit-server + index store.

---

## Wave 4 — SPIFFE / header auth

Depends on RANGER-5499 (already on 2.10).

| # | Status | SHA | Jira | Summary |
|---|--------|-----|------|---------|
| 4.1 | ✅ | `22fbc813` | RANGER-5700 | SPIFFE authn via HTTP headers |
| 4.2 | ✅ | `b222989f` | RANGER-5766 | SPIFFE IDs as usernames |
| 4.3 | ✅ | `d9d2c44d` | RANGER-5767 | ROLEs in header authn |
| 4.4 | ✅ | `8e7716cd3` | RANGER-5723 | SPIFFE outbound to audit-server |

**Gate:** Header auth in K8s-style setup; plugin → audit-server with SPIFFE headers.

**PR:** `backport-spiffe-2.10` → `ranger-2.10` (or stack on audit PR)

---

## Wave 5 — SQL / admin fixes

| # | Status | SHA | Jira | Summary |
|---|--------|-----|------|---------|
| 5.1 | ✅ | `06c44cbe` | RANGER-5736 | MariaDB grant fix |
| 5.2 | ✅ | `67233292` | RANGER-5693 | Stop logging full JWT (already on 2.10 via #1089) |
| 5.3 | ✅ | `b4fd13c20` | RANGER-5716 | KMS audit IP/resource fix |

**Gate:** MariaDB docker admin install; no JWT in logs on auth failure.

---

## Conflict resolution rules

1. **pom.xml / versions:** keep `2.10.0-SNAPSHOT` and 2.10 dependency versions.
2. **Jersey:** keep `com.sun.jersey` on 2.10 unless a commit is audit-only and needs glassfish — port minimally.
3. **GDS / 3.0-only files:** drop hunks entirely.
4. **JDK 17 / `--add-opens`:** drop unless required for 2.10 JDK 8 build.
5. Always `git cherry-pick -x` to record upstream SHA.

---

## Progress log

| Date | Wave | Action | Result |
|------|------|--------|--------|
| 2026-09-11 | 0 | Branch `backport-audit-spiffe-2.10` created | OK |
| 2026-09-11 | 1.1 | Cherry-pick `89ce14a26` RANGER-5482 | OK — 13 conflicts, resolved with `--theirs` |
| 2026-09-11 | 1.2 | Cherry-pick `3fd46dbe` RANGER-5520 | OK — 3 docker conflicts, manual merge |
| 2026-09-11 | 1.3–1.7 | 5613, 4676/5615, 5654, 5720 | OK — 5611 skipped |
| 2026-09-11 | GDS cleanup | Restore 2.10 `AssetMgr`, `RangerBizUtil`, `agents-common` from GDS pollution | OK — kept OpenSearch-only additions |
| 2026-09-11 | 2–5 | All cherry-picks applied on `backport-audit-spiffe-2.10` | OK |
| 2026-09-11 | Fix | Jersey 1 SPIFFE port, build fixes | OK — audit-server + dest modules build |
| 2026-09-11 | Build | `security-admin compile -am` | **PASS** — StopEmbeddedServer fix (ref: opensource master), GDS-free SPIFFE |
| 2026-09-11 | Docker | Smoke test | **PENDING** — run from `dev-support/ranger-docker` (see below) |

### Docker smoke test (from release + opensource README)

Working dir: `/Users/ramk/rangerRelease29/ranger/dev-support/ranger-docker`

```bash
# Build Ranger in docker (uses BRANCH=ranger-2.10 in .env; checkout backport branch first)
chmod +x scripts/**/*.sh download-archives.sh
./download-archives.sh hadoop hive hbase kafka knox ozone opensearch
export RANGER_DB_TYPE=postgres
docker compose -f docker-compose.ranger-build.yml build
docker compose -f docker-compose.ranger-build.yml up

# Full stack with audit-server + OpenSearch index store (5680 layout)
export AUDIT_DESTINATIONS=audit-store-opensearch
docker compose --profile ${AUDIT_DESTINATIONS} \
  -f docker-compose.ranger.yml \
  -f docker-compose.ranger-audit-service.yml \
  up -d
```

Verify: admin UI, plugins, audit-ingestor, OpenSearch `ranger_audits` index, audit logs in Admin.

---

## References

- [Ranger Release Process](https://apache.github.io/ranger/project/release-process/)
- [Internal RM checklist](https://cloudera.atlassian.net/wiki/spaces/ENG/pages/12303862298/Apache+Ranger+2.10+step-by-step+RM+checklist)
- Master audit-server log: `git log origin/ranger-2.10..origin/master --oneline -- audit-server/`
