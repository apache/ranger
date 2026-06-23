# RANGER-5655 — Pull Request Template

**GitHub PR:** https://github.com/apache/ranger/pull/1032  
**Jira:** [RANGER-5655](https://issues.apache.org/jira/browse/RANGER-5655) — Improvement · Major · Fix Version 3.0.0  
**Confluence:** [Dynamic Ingestor Registry Guide (Ranger Audit Ingestor)](https://cloudera.atlassian.net/wiki/spaces/ENG/pages/12043681813/Dynamic+Ingestor+Registry+Guide+Ranger+Audit+Ingestor)

Copy the section between **START / END** markers into the GitHub PR description.

---

## GitHub PR description (copy from here)

<!-- START: paste into GitHub PR description -->

**Jira:** [RANGER-5655](https://issues.apache.org/jira/browse/RANGER-5655)  
**Confluence:** [Dynamic Ingestor Registry Guide (Ranger Audit Ingestor)](https://cloudera.atlassian.net/wiki/spaces/ENG/pages/12043681813/Dynamic+Ingestor+Registry+Guide+Ranger+Audit+Ingestor)  
**Full template in repo:** `dev-support/RANGER-5655-PR-TEMPLATE.md`

## What changes were proposed in this pull request?

Implements [RANGER-5655](https://issues.apache.org/jira/browse/RANGER-5655): a **dynamic unified ingestor registry** for Ranger audit-ingestor so operators can change **Kafka partition routing** and **per-repo service allowlists** at runtime — without restarting ingestor pods.

The registry is a versioned JSON document in Kafka topic `ranger_audit_partition_plan` (**1 partition**, compacted). All ingestor replicas converge via `PartitionPlanWatcher`; `AuditPartitioner` routes on the hot path from in-memory state only.

**Feature flag (default off):** `ranger.audit.ingestor.kafka.partition.plan.dynamic.enabled=false`

### Problem

| Job | Static behavior today | Pain |
|-----|----------------------|------|
| Service allowlist | `ranger.audit.ingestor.service.*.allowed.users` in site XML at startup | Onboard repo / change allowlist → XML edit + restart all ingestor pods |
| Partition routing | `kafka.configured.plugins` + per-plugin overrides at startup | Promote hot plugin or grow partitions → restart; contiguous ranges can reshuffle later plugins |

### Solution

| Component | Role |
|-----------|------|
| `ranger_audit_partition_plan` | Compacted Kafka registry (key = audit topic name, value = plan JSON) |
| `PartitionPlanWatcher` + `PartitionPlanUpdateApplier` | Replay plan topic at startup; poll for updates; install in `PartitionPlanHolder` |
| `AuditPartitioner` | Dynamic mode: round-robin for promoted plugins; sticky hash for buffer; `max(cluster, plan)` after scale when metadata lags |
| `PartitionPlanService` + `AuditREST` | `GET` / `PATCH` / promote / scale / onboard with optimistic locking (`expectedVersion`) |
| `ServiceAllowlistResolver` + `AuthToLocalRuleComposer` | Registry-first allowlist; dynamic `auth_to_local` from allowlist union |
| `KafkaAuditTopicPartitionGrower` | Grows `ranger_audits` before plan references new tail partition ids |
| `PartitionPlanBootstrap` | First pod seeds initial plan from XML when registry is empty |

**Out of scope for this PR (follow-up):** Solr dispatcher Kerberos config, Docker E2E harness scripts, audit-server README docs.

---

## Code changes (52 files)

### `audit-common` (3 files)

| File | Change |
|------|--------|
| `AuditServerConstants.java` | `kafka.partition.plan.*` properties; plan topic constants |
| `AuditMessageQueueUtils.java` | `createPartitionPlanTopicIfNotExists()` — 1 partition, `cleanup.policy=compact` |
| `AuditMessageQueueUtilsTest.java` | Admin client config helper test |

### `audit-ingestor` — new package `...kafka.partition` (28 production Java)

| Class | Role |
|-------|------|
| `PartitionPlan`, `PluginPartitionAssignment`, `ServiceAllowlistEntry` | Plan JSON model |
| `OnboardService`, `PromotePlugin`, `PluginScale`, `PartitionPlanReplacement` | REST request DTOs |
| `PartitionPlanHolder` | In-memory plan snapshot (produce hot path) |
| `PartitionPlanWatcher`, `PartitionPlanUpdateApplier` | Kafka plan sync |
| `KafkaPartitionPlanRegistry`, `PartitionPlanRegistry*` | Read/write plan to Kafka |
| `PartitionPlanBootstrap`, `PartitionPlanBootstrapConfig` | XML → initial plan (greenfield) |
| `PartitionPlanAllocator`, `PartitionPlanValidator`, `PartitionPlanRequestValidator` | Append-only promote/scale/replace validation |
| `PartitionPlanService` | REST mutations + topic grow + read-back verify |
| `KafkaAuditTopicPartitionGrower` | AdminClient partition grow |
| `ServiceAllowlistBootstrap`, `ServiceAllowlistResolver` | Allowlist bootstrap/merge + registry-first lookup |
| `AuthToLocalRuleCatalog`, `AuthToLocalRuleComposer`, `PrimaryCatalogRule` | Dynamic auth_to_local from allowlists |
| `PartitionPlanKafkaConfig`, `PartitionPlanConstants` | Config resolution |
| `PartitionPlanException`, `PartitionPlanConflictException` | Error types |

### `audit-ingestor` — wiring (5 files)

| File | Change |
|------|--------|
| `AuditPartitioner.java` | Dynamic branch: `partitionFromPlan`, dedicated lanes + buffer pool |
| `AuditMessageQueue.java` | Ensure plan topic; start watcher; bootstrap when registry empty |
| `AuditREST.java` | Partition-plan REST + allowlist delegation |
| `AuditServerConfig.java` | Spring bean for `PartitionPlanService` |
| `ranger-audit-ingestor-site.xml` | Commented dynamic plan property block (default off) |

### Unit tests (15 files)

| Test class | Covers |
|------------|--------|
| `AuditPartitionerDynamicTest` | Round-robin, buffer hash, post-scale tail id when metadata lags |
| `PartitionPlanBootstrapTest`, `PartitionPlanBootstrapSupportTest` | Greenfield bootstrap, concurrent cold start |
| `PartitionPlanAllocatorTest` | Promote, scale, append-only tail growth |
| `PartitionPlanValidatorTest` | Duplicate ids, illegal reshuffle |
| `PartitionPlanServiceTest`, `PartitionPlanServiceMutationTest` | REST mutations; **409** on stale `expectedVersion` |
| `PartitionPlanHolderTest`, `PartitionPlanUpdateApplierTest` | Holder install; watcher record apply |
| `PartitionPlanKafkaConfigTest`, `PartitionPlanReplacementTest`, `PartitionPlanJsonTest` | Config + JSON serde |
| `ServiceAllowlistBootstrapTest`, `ServiceAllowlistResolverTest` | Allowlist merge + registry/XML fallback |
| `AuthToLocalRuleComposerTest` | auth_to_local composition from allowlists |

---

## REST API

Base path: `/api/audit` (same auth as other ingestor APIs).

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/partition-plan` | In-memory plan on this pod |
| `PATCH` | `/partition-plan` | Partial plan replace (`expectedVersion` required) |
| `POST` | `/partition-plan/plugins` | Promote plugin from buffer |
| `PATCH` | `/partition-plan/plugins/{pluginId}` | Scale plugin (append tail partitions) |
| `POST` | `/partition-plan/services` | Onboard repo: allowlist + promote in one version |

| HTTP status | When |
|-------------|------|
| `200` | Success |
| `400` | Invalid plan / validation failure |
| `409` | Version conflict |
| `503` | Dynamic mode disabled or plan not loaded |

---

## Configuration

| Property | Default | Description |
|----------|---------|-------------|
| `ranger.audit.ingestor.kafka.partition.plan.dynamic.enabled` | `false` | Master feature flag |
| `ranger.audit.ingestor.kafka.partition.plan.topic` | `ranger_audit_partition_plan` | Compacted registry topic |
| `ranger.audit.ingestor.kafka.partition.plan.refresh.interval.ms` | `30000` | Watcher refresh interval |

Static XML properties remain used for **initial bootstrap** when the registry is empty.

---

## How was this patch tested?

### Unit tests

```bash
mvn test -pl audit-server/audit-common,audit-server/audit-ingestor -Drat.skip=true
```

| Gate | Result |
|------|--------|
| `audit-ingestor` partition-plan + allowlist tests | **Pass** (15 test classes in this PR) |
| `AuditPartitionerDynamicTest` | **5/5 pass** — round-robin, buffer, post-scale routing |
| `audit-common` `AuditMessageQueueUtilsTest` | **Pass** |

Focused run:

```bash
mvn test -pl audit-server/audit-ingestor \
  -Dtest='AuditPartitionerDynamicTest,PartitionPlan*Test,ServiceAllowlist*Test,AuthToLocalRuleComposerTest' \
  -Drat.skip=true
```

### Manual testing (local Docker audit lab)

Manual validation was done on a **local Docker Compose audit stack** — a developer integration environment that mirrors a real Ranger audit deployment in miniature. It is **not** a production installer; it wires together the same components an operator would run in the field so we can exercise REST APIs, Kafka routing, Kerberos, and full audit delivery end to end.

**Reference:** [RANGER-5655](https://issues.apache.org/jira/browse/RANGER-5655) acceptance criteria · [Confluence — Dynamic Ingestor Registry Guide](https://cloudera.atlassian.net/wiki/spaces/ENG/pages/12043681813/Dynamic+Ingestor+Registry+Guide+Ranger+Audit+Ingestor)

#### What the lab contains

| Component | Role in testing |
|-----------|-----------------|
| **Kerberos (KDC)** | Plugins and ingestor authenticate with real SPNEGO principals (not mock users) |
| **Kafka** | Audit bus topic `ranger_audits` + registry topic `ranger_audit_partition_plan` (1 partition, compacted) |
| **audit-ingestor** | Receives plugin audits on `:7081`; partition-plan REST API; dynamic routing when flag is on |
| **Second ingestor (optional)** | Replica on `:7082` — proves all pods converge on the same plan without restart |
| **Solr + ZooKeeper** | Audit search backend; verify events appear after full pipeline runs |
| **Audit dispatchers** | Consume `ranger_audits` and write to Solr/HDFS — unchanged by this feature |
| **Ranger Admin + Postgres** | Policy store; Admin **Audit → Access** UI to confirm events |
| **Plugin containers** | HDFS (`ranger-hadoop`), Ozone, Hive, HBase, Kafka authorizer, Knox, KMS — each posts audits to ingestor for its repo |

**Typical lab layout for partition tests:** 13 configured plugins × 3 partitions each + 9 buffer partitions = **48** partitions on a clean `ranger_audits` topic. Plan watcher refresh interval: **30 seconds** (default).

**How to bring it up** (companion dev branch — scripts not in this 52-file PR):

```bash
cd dev-support/ranger-docker
export RANGER_DB_TYPE=postgres
./setup-audit-e2e.sh          # full stack: Admin, KDC, Kafka, Solr, ingestor, dispatchers, plugins
./scripts/audit/wait-for-audit-health.sh
```

---

#### Test block A — Partition plan REST and ingestor behavior

These tests focus on **ingestor + Kafka only**: turning dynamic mode on/off, the plan registry topic, REST mutations, and multi-pod sync. They do **not** require every plugin pipeline to run.

**Script:** `verify-partition-plan-e2e.sh`, `verify-partition-plan-e2e-all.sh`, `verify-partition-plan-multipod-e2e.sh`, `verify-partition-plan-brownfield-e2e.sh`

##### A1. Static mode still works (feature off — default)

| Step | Action | Expected | Result |
|------|--------|----------|--------|
| 1 | Leave `ranger.audit.ingestor.kafka.partition.plan.dynamic.enabled=false` (default) | Ingestor starts normally | **Pass** |
| 2 | `GET https://…:7081/api/audit/partition-plan` (Kerberos) | **503** — dynamic plan API not enabled | **Pass** |
| 3 | `GET /api/audit/health` | **200** — ingestor healthy | **Pass** |
| 4 | Trigger HDFS audit (`hdfs dfs -ls` as test user) | Event reaches Solr; Admin Access tab shows new row | **Pass** |

**Proves:** Existing deployments with the flag off behave exactly as before. No plan topic, no watcher, no REST side effects.

##### A2. Greenfield — first enable on empty plan topic

| Step | Action | Expected | Result |
|------|--------|----------|--------|
| 1 | Reset `ranger_audits` to known partition count (48); delete plan topic if present | Clean slate | **Pass** |
| 2 | Set `dynamic.enabled=true`; restart ingestor | Ingestor healthy; logs show `PartitionPlanWatcher` started | **Pass** |
| 3 | `kafka-topics --describe ranger_audit_partition_plan` | Topic exists; **PartitionCount: 1**; `cleanup.policy=compact` | **Pass** |
| 4 | `GET /partition-plan` | JSON **version 1**; 13 plugins listed; `topicPartitionCount` = **48** | **Pass** |
| 5 | Compare plan count vs `kafka-topics --describe ranger_audits` | Numbers **match** | **Pass** |
| 6 | Ingestor startup log | `Mode: dynamic (PartitionPlanHolder)` with plugin ranges and buffer pool | **Pass** |

**Proves:** On a new cluster, the **first ingestor pod** auto-creates version 1 of the plan from existing XML (`kafka.configured.plugins`, buffer size, per-plugin overrides) — no manual REST call required.

##### A3. Change routing live via REST (no ingestor restart)

| Step | Action | Expected | Result |
|------|--------|----------|--------|
| 1 | `POST /partition-plan/plugins` — promote buffer plugin `storm`, `partitionCount: 2`, correct `expectedVersion` | **200**; version increments; `storm` appears under `plugins` | **Pass** |
| 2 | Same promote with stale `expectedVersion: 1` | **409 Conflict** — forces admin to refresh plan | **Pass** |
| 3 | `POST /plugins` for `hdfs` (already has dedicated partitions) | **400 Bad Request** | **Pass** |
| 4 | `PATCH /partition-plan/plugins/storm` — `additionalPartitions: 1` | **200**; tail partition appended; `ranger_audits` grown via AdminClient if needed | **Pass** |
| 5 | `GET /partition-plan` again **without restart** | Same version and layout as last mutation | **Pass** |

**Proves:** Operators can promote and scale plugins at runtime. Append-only — existing plugin partition lists are never reshuffled.

##### A4. Two ingestor pods stay in sync

| Step | Action | Expected | Result |
|------|--------|----------|--------|
| 1 | Start second ingestor replica on **:7082** (separate hostname + Kerberos identity, same Kafka) | Replica healthy; watcher ready | **Pass** |
| 2 | `GET /partition-plan` on **:7081** and **:7082** | **Same version** at start | **Pass** |
| 3 | Promote a buffer plugin on **primary (:7081) only** | Primary returns new version | **Pass** |
| 4 | Wait **≤ 35 s** (one watcher cycle); `GET /partition-plan` on **:7082** | Replica shows **same new version** — no REST call on replica | **Pass** |

**Proves:** In a multi-pod cluster behind a load balancer, a routing change propagates to all ingestors automatically within one refresh interval.

##### A5. Brownfield cutover — pre-load plan before enabling dynamic

| Step | Action | Expected | Result |
|------|--------|----------|--------|
| 1 | Capture plan JSON while dynamic is briefly on | Valid layout saved | **Pass** |
| 2 | Disable dynamic; delete plan topic | Static mode healthy | **Pass** |
| 3 | Operator pre-seeds plan to Kafka with `updatedBy=brownfield-e2e-seed`, `version: 1` | Message on plan topic | **Pass** |
| 4 | Re-enable dynamic; restart ingestor | Plan shows **brownfield-e2e-seed** — **not** overwritten by XML bootstrap | **Pass** |
| 5 | Rollback: `dynamic.enabled=false` | `GET /partition-plan` → **503**; health **200** | **Pass** |

**Proves:** Production cutover path works — operator writes the plan first, then enables the feature.

##### A6. Kafka down at startup (dynamic on)

| Step | Action | Expected | Result |
|------|--------|----------|--------|
| 1 | Stop Kafka broker; restart ingestor with `dynamic.enabled=true` | Health **not** OK; logs show Kafka / plan watcher failure | **Pass** |
| 2 | Start Kafka; disable dynamic; restart ingestor | Health **200** in static mode | **Pass** |

**Proves:** Dynamic mode fails visibly when Kafka is unavailable — no silent fallback to a broken state.

**Block A summary:** **31 automated checks** in the partition-plan script suite — **all passed** (see companion branch validation report).

---

#### Test block B — Full plugin audit pipelines + dynamic routing

These tests prove the **whole audit path** still works after dynamic partition changes: real plugin operation → Kerberos POST to ingestor → correct Kafka partition → dispatcher → Solr → Admin UI.

**Scripts:** `verify-hdfs-dynamic-partition-e2e.sh`, `verify-dynamic-partition-plugin-e2e.sh`, `verify-dynamic-auth-to-local-e2e.sh`, `verify-audit-e2e-full.sh`

##### B1. HDFS — onboard, route, scale, re-route

| Step | Action | Expected | Result |
|------|--------|----------|--------|
| 1 | Enable dynamic mode (greenfield buffer layout) | Watcher ready | **Pass** |
| 2 | `POST /partition-plan/services` — repo `dev_hdfs`, plugin `hdfs`, allowlist `hdfs`, dedicated partitions | **200**; plan version bumps | **Pass** |
| 3 | Plugin principal POST `/api/audit/access` (SPNEGO) | **200**; `authenticatedUser=hdfs` | **Pass** |
| 4 | Inspect Kafka record for that event | Partition number ∈ `hdfs` assignment list in plan (not buffer) | **Pass** |
| 5 | `PATCH /partition-plan/plugins/hdfs` — add 2 tail partitions | **200**; `ranger_audits` grown; plan lists new ids | **Pass** |
| 6 | Repeat POST `/access` + Kafka inspect | Still lands on an `hdfs` partition from updated plan | **Pass** (routing); Step 5 marker lookup had **one harness flake** on post-scale Kafka poll |
| 7 | Optional: `hdfs dfs -ls /user/` smoke | Full 6-hop: plugin → ingestor → Kafka → dispatcher → Solr → Admin Access | **Pass** when HDFS dispatcher healthy |

**Proves:** Real HDFS audits follow the live partition plan before and after scale — the core value of dynamic allocation.

##### B2. Allowlist + auth_to_local (who may POST audits)

| Step | Action | Expected | Result |
|------|--------|----------|--------|
| 1 | Plugin calls POST `/access` with Kerberos principal (e.g. `hdfs/ranger-hadoop@REALM`) | Ingestor maps via `auth_to_local` → short name `hdfs` → **200** if in `services[dev_hdfs].allowedUsers` | **Pass** |
| 2 | Remove user from allowlist via `PATCH /partition-plan` (services delta only) | Same principal → **403** | **Pass** |
| 3 | Re-add allowlist | **200** again | **Pass** |
| 4 | HDFS principal posting to wrong repo (e.g. `dev_kms`) | **403** cross-repo denial | **Pass** |

**Proves:** The unified registry `services` map controls authorization without XML restart; `auth_to_local` rules stay aligned with allowlists in dynamic mode.

##### B3. Other plugins (Ozone, Hive, HBase, Kafka authorizer)

Same pattern as HDFS: enable dynamic → `POST /partition-plan/services` for repo + plugin + allowlist → POST `/access` with plugin keytab → verify Kafka partition ∈ plan → optional full pipeline to Solr.

| Plugin | Repo | Principals tested | Pipeline to Solr |
|--------|------|-------------------|------------------|
| Ozone | `dev_ozone` | `om`, `ozone` | **Pass** |
| Hive | `dev_hive` | `hive` | **Pass** |
| HBase | `dev_hbase` | region/master principals | **Pass** |
| Kafka authorizer | `dev_kafka` | kafka plugin principal | **Pass** |

**Proves:** Dynamic partition plan does not break audit delivery for multiple plugin types.

---

#### What was not manually tested

| Topic | Why |
|-------|-----|
| Two pods cold-starting **simultaneously** on empty plan topic | Choreographed parallel rollout; covered in unit test `PartitionPlanBootstrapSupportTest` |
| Dedicated **403** for non-admin on partition-plan REST | Deferred — admin allow-list phase |
| Shrinking `ranger_audits` partition count | Not supported by Kafka |
| Production multi-broker failure injection | Single-broker Docker lab only |

**Note:** Docker E2E scripts live in the full development branch (`dev-support/ranger-docker/scripts/audit/`), not in this 52-file PR. Reproduce using the commands above and the [Confluence guide](https://cloudera.atlassian.net/wiki/spaces/ENG/pages/12043681813/Dynamic+Ingestor+Registry+Guide+Ranger+Audit+Ingestor).

### Not in this PR

| Item | Notes |
|------|-------|
| Solr `useTicketCache=false` (RANGER-5654) | Separate PR |
| Docker E2E script bundle | Follow-up PR |
| audit-server README docs | Follow-up PR |
| Dedicated admin allow-list for partition-plan REST | Deferred |

---

## Backward compatibility

- Feature flag **off** by default — no new Kafka topic, no watcher, no REST side effects.
- Static `AuditPartitioner` code path unchanged when flag is false.
- Solr/HDFS dispatchers unchanged (consume `ranger_audits` as before).

---

## Checklist

- [x] [RANGER-5655](https://issues.apache.org/jira/browse/RANGER-5655) linked
- [x] [Confluence guide](https://cloudera.atlassian.net/wiki/spaces/ENG/pages/12043681813/Dynamic+Ingestor+Registry+Guide+Ranger+Audit+Ingestor) referenced
- [x] Feature flag defaults to `false`
- [x] Unit tests pass for `audit-common` + `audit-ingestor`
- [x] Manual Docker audit lab scenarios exercised (partition REST + full plugin pipelines — see template)
- [ ] Reviewer: confirm static-mode regression on your cluster

<!-- END: paste into GitHub PR description -->
