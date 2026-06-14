<!--
SPDX-License-Identifier: Apache-2.0

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Apache Ranger — Threat Model

## §1 Header

- **Project:** Apache Ranger
- **Repository:** https://github.com/apache/ranger
- **Version/commit modeled against:** `master` branch as of 2026-06-03 (no specific release tag; this v0 predates maintainer review).
- **Threat-model authors:** Drafted by the ASF Security team as a v0 pre-flight artifact for an automated agentic security scan; **reviewed by the Apache Ranger PMC (Abhishek Kumar, `@kumaab`) on 2026-06-12**, who answered every §14 question — those answers are folded into §1–§13 below.
- **Date:** 2026-06-03 (v0); 2026-06-12 (v1, PMC-reviewed).
- **Status:** v1 — PMC-reviewed. All §14 questions were answered by the Ranger PMC on 2026-06-12 and folded in; the claims they confirm are promoted to *(maintainer)*. Abhishek Kumar (PMC) volunteered to own ongoing revision (§14 Q20).

**Version binding.** Once ratified, this model is intended to be versioned alongside Ranger and tagged with releases. A report against Ranger version *N* should be triaged against the model as it stood at *N*, not at HEAD. This v0 carries no such binding yet.

**Reporting cross-reference.** Apache Ranger has no `SECURITY.md` in the repository (confirmed 404) and no dedicated security/threat-model page on https://ranger.apache.org/ at the time of writing *(documented — absence verified)*. Vulnerabilities in Apache projects are reported to `security@ranger.apache.org` / `security@apache.org` per the ASF-wide process. Findings that fall under §8 (claimed properties) should be reported through that channel; findings that fall under §3 or §9 will be closed citing this document once it is ratified.

**Provenance legend.** Every non-trivial claim carries exactly one tag:
- *(documented)* — stated in Ranger's own docs (README, ranger.apache.org, FAQ, API docs) or directly verifiable from the public repository layout; cited inline.
- *(maintainer)* — stated by a Ranger maintainer in response to this process. As of 2026-06-12 the PMC has answered all 20 §14 questions (see §14); the load-bearing claims they confirmed are tagged *(maintainer, 2026-06-12)*.
- *(inferred)* — reasoned from code/repo structure, absence of a feature, or general domain knowledge. Remaining *(inferred)* tags are claims the PMC has not yet individually ratified beyond the §14 answers.

**Confidence:** PMC-reviewed v1 — all 20 §14 questions answered by the maintainer on 2026-06-12 and folded into §1–§13. A handful of items (e.g. the "no resource guarantee" line, §8 #6) were explicitly deferred by the PMC for later ratification.

**What Apache Ranger is.** Apache Ranger is "a framework to enable, monitor and manage comprehensive data security across 20+ data processing services" *(documented — ranger.apache.org)*. It provides centralized & fine-grained authorization, audit, and key management for data services (Trino, Polaris, Ozone, HDFS, Hive, HBase, Kafka, NiFi, Knox, Kudu, YARN, Solr, Atlas and others). The administrator defines access-control policies in a central web application (Ranger Admin); lightweight per-service Java plugins, deployed inside each data service's own process, pull those policies and enforce authorization decisions locally on every access request. In threat-model terms Ranger is a **distributed Policy Decision / Policy Enforcement system**: the policy decision authority lives with the admin server (which authors and distributes policy), but the actual *enforcement* (the PEP) runs as plugin code co-located inside the data service it guards *(documented — FAQ: plugins "run as part of the same process as the namenode (HDFS), Hive2Server(Hive), HBase server (Hbase)")*.

---

## §2 Scope and intended use

**Primary intended use.** Centralized authoring of fine-grained authorization policies (resource-based and tag-based) and their enforcement across many independent data services, plus unified audit of access decisions and a key-management service (Ranger KMS) for HDFS transparent encryption *(documented — ranger.apache.org goals: "Centralized security administration", "Fine grained authorization", "Centralize auditing of user access")*.

**Deployment context.** Ranger is **not** an in-process library. It is a multi-component distributed system deployed across a cluster: a central web application/daemon (Ranger Admin, default port 6080 *(documented — README)*), a user/group sync daemon, an optional tag-sync daemon, the Ranger KMS daemon, and a fleet of plugins each embedded inside a separate data-service process on separate (typically trusted, intra-cluster) hosts *(documented — README lists Admin Tool, User Synchronization, and component plugins as the deployment tiers)*.

**Caller roles.** Because Ranger is a distributed service rather than a single library, "the caller" splits into distinct roles, each modeled separately in §6/§7:

- **Security administrator** — authors policies, manages users/roles, views audits via the Admin UI or REST API. Trusted for the instance.
- **Delegated administrator** — a group owner to whom administration of a subset of resources has been delegated *(documented — FAQ: "delegate administration of certain data to other group owners")*. Trusted for their delegated scope only.
- **Deployed plugin (PEP)** — code running inside a data service, authenticated to the Admin server, that downloads policy and enforces it. **Fully trusted once authenticated** — a deliberate design choice (§7) *(maintainer, 2026-06-12 — §14 Q8)*.
- **End user of the guarded data service** — the principal whose HDFS/Hive/etc. access Ranger authorizes. **Untrusted** with respect to the decisions Ranger makes about them; they do not interact with Ranger directly but their identity and requested action are the inputs to the PEP. Ranger does **not** authenticate the end user at access time — the host service does *(maintainer, 2026-06-12 — §14 Q7)*.
- **Identity source** — LDAP/AD/Unix that usersync reads from. Trusted as the authority for user/group membership *(maintainer, 2026-06-12 — §14 Q16)*.
- **Auditor** — may view policies and access audits but not author them (writes are blocked) *(maintainer, 2026-06-12 — §14 Q5/Q10)*.
- **Key Admin** — administers Ranger KMS keys only *(maintainer, 2026-06-12 — §14 Q5)*.

**Component-family table.**

| Family | Representative entry point | Touches outside its process | In this model? |
| --- | --- | --- | --- |
| Ranger Admin web app | `security-admin/`, `agents-common/`, REST `/service/...`, `/public/v2/api/...` *(documented — API docs)* | DB, network (serves plugins + UI), LDAP/AD | **In** |
| PDP service | `pdp/`, `authz-api/`, `authz-embedded/`, `authz-remote/` | Network (serves remote authz API); Admin (policy pull); audit sink | **In** *(maintainer, 2026-06-12 — §14 Q6/Q10)* |
| Per-service plugins (PEPs) | `*-agent/` (`hdfs-agent/`, `hive-agent/`, `hbase-agent/`), `plugin-*/` (`plugin-kafka/`, `plugin-nifi`, `plugin-ozone`, `plugin-trino`, `plugin-kms`), `agents-common/` | Embedded in host service; network to Admin; audit sink | **In** |
| Plugin shims / classloader | `ranger-*-plugin-shim/`, `ranger-plugin-classloader/` | Class loading inside host service | **In** (as part of PEP delivery) |
| User/group sync | `ugsync/`, `ugsync-util/` | LDAP/AD/Unix; writes to Admin DB | **In** |
| Tag sync | `tagsync/` | Atlas/Kafka; writes to Admin | **In** |
| Audit framework | `agents-audit/`, `audit-server/` | Solr/OpenSearch/Kafka/HDFS/DB audit sinks | **In** |
| Ranger KMS | `kms/`, `plugin-kms/` | Key store (DB/HSM), HDFS NameNode/DataNode | **In** (model at its own trust level — handles key material) |
| Authentication | `ranger-authn/`, `unixauthservice/`, `agents-cred/`, `credentialbuilder/` | Kerberos/SPNEGO/LDAP; credential stores | **In** |
| Examples / sample app + tools | `ranger-examples/`, `ranger-tools/` *(documented — repo layout)* | Demo / utility | **In** *(maintainer, 2026-06-12 — §14 Q4)* |
| Build / install / migration / ugsync tooling | `distro/`, `agents-installer/`, `migration-util/`, ugsync `filesourceusersynctool`/`ldapconfigchecktool`, `dev-support/` | Build/deploy host | **Out** *(maintainer, 2026-06-12 — §14 Q4)* |

---

## §3 Out of scope (explicit non-goals)

- **Build, packaging, install, and migration tooling** (`distro/`, `agents-installer/`, `migration-util/`, Docker build scripts) **and the ugsync utilities `filesourceusersynctool` / `ldapconfigchecktool`.** SDLC/deployment hygiene, not the runtime trust surface *(maintainer, 2026-06-12 — §14 Q4)*. **Note:** unlike a typical project, `ranger-examples/` and `ranger-tools/` are **IN** scope here per the PMC (§2) — do not treat example/tool code as out-of-model.
- **The guarded data services themselves.** Ranger authorizes access *within* HDFS, Hive, HBase, etc., but it does not own those services' own attack surface (e.g., an HDFS RPC bug). Ranger's responsibility begins at the authorization decision and ends at returning allow/deny to the host service *(inferred)*.
- **Authentication of end users.** Ranger authorizes an *already-authenticated* principal; establishing that the principal is who they claim (Kerberos ticket validation, etc.) is performed by the host data service / cluster Kerberos infrastructure, not by Ranger's PDP. Ranger trusts the identity the PEP presents *(inferred)*.
- **Network transport security as a Ranger guarantee.** Whether plugin↔Admin and UI↔Admin traffic is TLS-protected is a deployment configuration concern; Ranger supports it but does not enforce it at the protocol layer *(inferred)*.
- **Correctness/security of third-party identity stores** (LDAP/AD) and audit/key backends (Solr, HDFS, RDBMS, HSM). Ranger integrates with them but does not model their internal threats *(inferred)*.
- **Defense against a fully-compromised host running a plugin.** A plugin runs inside the data-service process on a cluster node; an attacker with code execution on that node has already defeated the PEP locally (see §7) *(inferred)*.

---

## §4 Trust boundaries and data flow

**Where the boundary sits.** Ranger has several trust boundaries, not one:

1. **End user ↔ guarded data service (the access request).** The user's identity + requested resource/action enters the host service, which calls the embedded Ranger plugin (PEP). This is the primary *untrusted-input* boundary: the requested resource name and action are derived from an untrusted user, though the *identity* is asserted by the host service's authentication layer, which Ranger trusts *(inferred)*.

2. **Plugin (PEP) ↔ Ranger Admin (policy distribution).** Plugins "pull the policy-changes using REST API at a configured regular interval (e.g.: 30 second)" *(documented — FAQ)*. The plugin trusts that the policy it downloads genuinely originates from the legitimate Admin server, and the Admin trusts that only authorized plugins download policy. This boundary is authenticated via **Kerberos, JWT, or header-based auth with a trusted proxy** — the policy-download path (and the separate PDP service process) requires an authenticated identity *(maintainer, 2026-06-12 — §14 Q6/Q10)*. Plugins cache the last-known policy and "function even if the policy server is temporarily down" *(documented — FAQ)*, so the cache is load-bearing.

3. **Admin UI / REST client ↔ Ranger Admin (administration).** Administrators author policy and read audits over `/service/...` and `/public/v2/api/...` REST endpoints *(documented — API docs)*. This is the highest-value boundary: anyone who can author policy can grant themselves or others access to all guarded data.

4. **Usersync/tagsync ↔ Admin, and Admin ↔ DB / audit sink / key store.** Internal data-plane boundaries; integrity of the user/group/role data and the policy store determines the integrity of every downstream decision *(inferred)*.

**Data flow (authorization path).** Untrusted user request → host service authenticates principal → host service calls embedded Ranger plugin with (principal, resource, action) → plugin evaluates locally-cached policy (resource-based + tag-based) → returns allow/deny → plugin emits an audit record to the configured sink → host service enforces the decision *(documented in part — FAQ; full flow inferred)*.

**Reachability preconditions per component (the triager's first test):**

- A finding in a **plugin / `agents-common`** is in-model only if reachable from a policy-evaluation input — i.e., from the (principal, resource, action) tuple of an untrusted access request, or from the downloaded policy document, or from the plugin's local config *(inferred)*.
- A finding in the **PDP service (`pdp/`, `authz-api/`, `authz-embedded/`, `authz-remote/`)** is reached over the network, not in-process. Its authorization API (`/authz/*`) requires an authenticated client (Kerberos/JWT/trusted-header; the server refuses to start with no auth handler configured), so treat it like an authenticated **Ranger Admin REST** endpoint — there is no anonymous path into the authz API. Only the liveness/readiness/metrics probes (`/health/live`, `/health/ready`, `/metrics`) are reachable unauthenticated, mirroring Admin's public health/metrics bucket (§14 Q10) *(inferred)*.
- A finding in **Ranger Admin REST** is in-model only if reachable by an actor the endpoint's auth permits — distinguish endpoints reachable by an *unauthenticated* network peer from those requiring an authenticated admin/plugin identity *(inferred)*.
- A finding in **usersync/tagsync** is in-model only if reachable from the external identity/tag source data or its configuration *(inferred)*.
- A finding in **Ranger KMS** is in-model only if reachable from a key-operation request or the key store contents *(inferred)*.
- A finding in **`ranger-examples/`** is out of model (§3) *(inferred)*.

---

## §5 Assumptions about the environment

- **Runtime.** JVM-based (Java). The README mandates `--add-opens=java.base/java.nio=ALL-UNNAMED` and related flags for supported operation *(documented — README)*, implying modern JDK module assumptions.
- **Network.** Components are assumed deployed within a cluster network where plugin nodes are administered hosts, not arbitrary internet-facing machines *(inferred)*. The Admin listens on 6080 by default *(documented — README)*.
- **Cluster identity infrastructure.** Ranger assumes an external authentication authority (typically Kerberos in a Hadoop cluster, plus LDAP/AD for user/group identity) establishes principal identity before authorization is requested *(inferred)*.
- **Persistent stores.** A relational database backs the Admin policy/audit/user store; audit may additionally target Solr and/or HDFS; KMS keys live in a DB or HSM. Ranger assumes these stores are themselves access-controlled and integrity-preserving *(inferred)*.
- **Concurrency.** The Admin is a multi-user web application; plugins serve concurrent authorization calls inside multi-threaded host services. Thread-safety of the policy-evaluation engine under concurrent reads + periodic policy refresh is assumed *(inferred)*.
- **Clock.** Time-bound policies (validity schedules) assume reasonably synchronized clocks across Admin and plugin nodes *(inferred)*.

**What Ranger does *not* do to its host (negative side-effect inventory — predominantly inference, high-priority confirmation target):**
- A plugin is assumed **not** to alter the host service's behavior beyond returning allow/deny + emitting audit (no spawning, no opening unrelated sockets, no mutating unrelated host state) *(inferred)*.
- The plugin is assumed **not** to phone home anywhere except the configured Admin server and audit sink *(inferred)*.
- Failure mode is assumed **fail-safe**: if policy cannot be evaluated, the documented behavior is to fall back to cached policy *(documented — FAQ)*; whether a total evaluation failure denies or defers to the host service's native ACLs is **unspecified here** *(inferred)*.

---

## §5a Build-time and configuration variants

Ranger's security envelope is governed primarily by **runtime configuration**, not compile-time flags. The variants that change which properties hold:

- **TLS on plugin↔Admin and UI↔Admin.** TLS is recommended in production, but **plaintext HTTP is the shipped and supported default** *(maintainer, 2026-06-12 — §14 Q2)*. Because plaintext is a *supported* posture, a report that reduces to "traffic is unencrypted by default" is `BY-DESIGN` / operator-hardening, not `VALID`.
- **Authentication mode for the Admin** (Kerberos/SPNEGO, LDAP/AD, Unix, PAM, Knox SSO, header/JWT, or internal DB) *(maintainer, 2026-06-12 — §14 Q7)*. Ranger seeds accounts (`admin`, `rangerusersync`, `rangertagsync`, `keyadmin`) at bootstrap, but **the installer mandates an explicit, complexity-checked password on fresh install** — a seeded/default password is **not** a supported production posture *(maintainer, 2026-06-12 — §14 Q3)*.
- **Audit destination** (Solr / HDFS / DB / log file / none). Disabling audit voids the §8 auditability property *(inferred)*.
- **No-match behavior:** when Ranger is the ACL enforcer and no policy matches, the result is **deny** — **except the HDFS service**, where it falls through to the host's native ACLs *(maintainer, 2026-06-12 — §14 Q1/Q9)*.
- **KMS key store** (DB-backed vs. HSM/KeySecure). Changes the confidentiality guarantee for key material *(inferred)*.

For each knob whose default is the less-secure value, the PMC's ruling (supported production posture → reports are `VALID`; vs. dev-convenience requiring operator action → `OUT-OF-MODEL: non-default-build`) is recorded once confirmed. **No ruling exists yet.**

---

## §6 Assumptions about inputs

Ranger is a network service, so the trust table is keyed by endpoint/message, not function. Header-presence and identity-assertion checks are the prime "false friends."

| Endpoint / input | Parameter | Attacker-controllable? | Caller/operator must enforce |
| --- | --- | --- | --- |
| Plugin authorization call (in-process, from host service) | principal identity | no — asserted by host service auth *(inferred)* | host service must authenticate before calling |
| Plugin authorization call | resource name, action | **yes** — derived from untrusted user request *(inferred)* | plugin must evaluate policy correctly against arbitrary resource strings |
| `GET /service/plugins/policies/download` (policy pull) | requesting plugin identity | no — must be authenticated plugin *(inferred)* | mutual auth between plugin and Admin |
| `POST/PUT /public/v2/api/policy` *(documented — API docs)* | policy body | no — trusted admin only *(inferred)* | endpoint must require admin authz; never expose unauthenticated |
| `/service/xusers/users`, `/groups`, `/roleassignments` *(documented — API docs)* | user/group/role data | partly — usersync feeds it from LDAP/AD *(inferred)* | trust in the identity source; admin authz on the endpoint |
| `/service/assets/accessAudit`, `/xaudit/...` *(documented — API docs)* | audit query params | depends on who can reach it | restrict audit read to authorized roles |
| Ranger KMS `/keys/key`, `/keys/keys` *(documented — API docs)* | key name, key ops | by an authenticated KMS client *(inferred)* | KMS must authorize each key operation against policy |
| Usersync input | LDAP/AD/Unix records | as trusted as the identity source *(inferred)* | secure + authenticate the directory connection |
| Tagsync input | tags from Atlas/Kafka | as trusted as the tag source *(inferred)* | secure the tag source channel |

**Size/shape/rate.** Policy documents downloaded by plugins, audit batches, and REST payload sizes are assumed bounded by normal operation; explicit limits are unconfirmed *(inferred)*.

---

## §7 Adversary model

**In-scope adversaries (hypothesized — all to be confirmed):**

- **An end user of a guarded data service** attempting to access data they are not authorized for, by manipulating the resource/action they request, exploiting policy-evaluation edge cases (resource-name matching, wildcard/recursion handling, case sensitivity, tag-vs-resource precedence), or attempting privilege escalation through Ranger's own access paths *(inferred)*.
- **A network peer on the cluster network** attempting to reach the Admin REST API or the policy-download endpoint without proper authentication, or to impersonate the Admin to a plugin / a plugin to the Admin *(inferred)*.
- **A low-privilege or delegated administrator** attempting to exceed their delegated scope — author policy outside their delegation, escalate their own role, or read audits/keys they should not *(inferred)*.
- **A malicious identity/tag source, or someone able to inject into usersync/tagsync inputs**, attempting to forge group/role membership and thereby gain policy grants *(inferred)*.
- **A KMS client** attempting to obtain key material for keys it is not authorized to use *(inferred)*.

**Attacker capabilities assumed.** Send arbitrary access requests to guarded services; reach Admin/KMS network endpoints; observe network traffic if transport is unprotected; supply arbitrary resource strings and policy/identity data at the boundaries marked attacker-controllable in §6 *(inferred)*.

**Explicitly out of scope (hypothesized):**
- **An attacker with code execution on a node running a plugin.** They are inside the PEP's own process/host and can bypass enforcement locally; Ranger cannot defend a PEP against its own compromised host *(inferred)*.
- **A fully-trusted security administrator acting maliciously.** The top-level admin is the root of authority for the Ranger instance; the model does not defend the guarded data against a malicious omnipotent admin (delegated admins overreaching *is* in scope) *(inferred)*.
- **Compromise of the backing database, audit store, or key store directly** (bypassing Ranger) — that is the store's own trust boundary *(inferred)*.
- **Side-channel / timing adversaries** against the policy engine or KMS *(maintainer, 2026-06-12 — §14 Q19: out of scope)*.

**Authenticated-but-Byzantine participant.** Ranger is a distributed PDP/PEP system: a plugin holds a legitimate service identity and could, if its host is compromised, behave arbitrarily (return wrong decisions, withhold audit). **Ranger treats every PEP as fully trusted once authenticated and makes no cross-node integrity claim** — complete plugin trust is a deliberate design choice that keeps runtime overhead low *(maintainer, 2026-06-12 — §14 Q8)*.

---

## §8 Security properties the project provides

*(All entries below are hypothesized for the PMC to confirm; none are maintainer-ratified yet.)*

1. **Authorization decisions reflect the authored policy.** Given correctly-distributed policy and a correctly-authenticated principal, a plugin's allow/deny matches what the central policy specifies for that (principal, resource, action). *Violation symptom:* a user is granted access the policy denies (or denied access the policy grants) — i.e., a policy-evaluation/matching bug. *Severity:* security-critical — **an existing deny-policy (or the absence of any grant) that nonetheless yields access is CVE-class `VALID`** *(maintainer, 2026-06-12 — §14 Q11)*.

2. **Centralized policy is faithfully distributed to enforcement points.** Policy authored in Admin is delivered to plugins via the periodic pull and applied; plugins fall back to last-cached policy when Admin is unreachable rather than failing open arbitrarily. *Violation symptom:* a plugin enforces stale or wrong policy in a way that grants access the current policy denies; or distribution lets an attacker substitute policy. *Severity:* security-critical. *(documented in part — FAQ on pull interval + cache fallback; the "faithfully/integrity" guarantee is inferred)*

3. **Administrative actions are access-controlled.** Authoring/modifying policy, managing users/roles, and reading audits via the REST API and UI require appropriate authenticated, authorized identity; delegated admins are confined to their delegated scope. *Violation symptom:* an unauthenticated or under-privileged actor authors/reads policy, escalates a role, or reads audit/keys. *Severity:* security-critical. *(documented in part — delegation in FAQ; enforcement details inferred)*

4. **Access decisions are audited.** Allow/deny decisions and administrative actions are recorded to the configured audit sink. *Violation symptom:* an in-scope access produces no audit record, or audit can be silently suppressed/forged by a non-admin. *Severity:* security-relevant — **a non-admin able to tamper or forge audit records IS a security finding** *(maintainer, 2026-06-12 — §14 Q14)*; missed records alone are correctness-grade. *(documented — ranger.apache.org: "Centralize auditing of user access and administrative actions")*

5. **KMS authorizes key access per policy.** Ranger KMS releases key material only to clients authorized by key-access policy. *Violation symptom:* unauthorized key retrieval / decryption capability. *Severity:* security-critical — **the key is the KMS-policy resource; unauthorized key retrieval is CVE-class `VALID`** *(maintainer, 2026-06-12 — §14 Q13)*.

6. **Policy evaluation is bounded and thread-safe.** Each authorization call terminates promptly and concurrent evaluation during a policy refresh does not corrupt decisions. *Violation symptom:* hang/CPU exhaustion inside the host service on crafted resource input, or a race producing a wrong decision. *Severity:* the engine is bounded and thread-safe *(maintainer, 2026-06-12 — §14 Q12)*. **Super-linear evaluation cost in policy size or resource-string length is NOT a bug**; a **hang inside the host service may be** a security issue; whether *no* resource guarantee is made was deferred by the PMC for later ratification.

---

## §9 Security properties the project does *not* provide

*(Hypothesized; confirm/correct.)*

- **No authentication of end users.** Ranger authorizes an already-authenticated principal; it does not establish identity. A forged/spoofed principal accepted by the host service's authentication layer is the host service's / cluster Kerberos's problem, not Ranger's *(inferred)*.
- **No protection against a malicious top-level administrator.** The admin is the root of trust for the instance *(inferred)*.
- **No defense of a plugin against its own compromised host.** A PEP running in a compromised data-service process can be bypassed locally *(inferred)*.
- **No transport security by default.** Plaintext HTTP is the supported default *(maintainer, 2026-06-12 — §14 Q2)*; Ranger supports TLS but does not enforce it at the protocol layer. Plaintext deployment exposes policy and audit in transit.
- **No guarantee about the guarded service's own attack surface.** Ranger only renders authorization decisions *(inferred)*.

**False friends (highest-value warnings — confirm):**
- **A Ranger "deny" is not a sandbox.** Enforcement depends on the host service actually calling the plugin for every access path. A code path in the guarded service that does not consult the plugin is not protected by Ranger *(inferred)*.
- **Cached policy is availability, not authority.** The "works when Admin is down" cache *(documented — FAQ)* means a plugin can enforce *stale* policy: a just-revoked grant may persist until the next successful pull. Operators must not treat revocation as instantaneous — the propagation delay is **by design** *(maintainer, 2026-06-12 — §14 Q15)*.
- **Audit is a record, not a control.** An audit entry does not prevent access; it documents it. Absence of an audit record is not proof access was blocked *(inferred)*.
- **Tag-based policy depends on the freshness/integrity of tagsync.** A policy keyed on a classification tag is only as trustworthy as the tag feed from Atlas *(inferred)*.

**Well-known attack classes left to the integrator/operator:**
- Policy substitution / MITM on the plugin↔Admin channel if transport is unprotected *(inferred)*.
- Identity/group spoofing upstream of Ranger (forged Kerberos/LDAP membership) *(inferred)*.
- Resource-name canonicalization mismatches between the host service and the policy engine (path normalization, case, wildcards) leading to bypass *(inferred)*.
- Audit-store injection (e.g., log/Solr injection via crafted resource names) *(inferred)*.

---

## §10 Downstream responsibilities

For Ranger the "downstream user" is the **cluster operator** who deploys Admin + plugins. To keep §5–§7 assumptions valid, the operator must (hypothesized — confirm):

- Deploy plugins only on trusted, administered cluster nodes inside the trust boundary; never expose the Admin REST API or policy-download endpoint to untrusted networks *(inferred)*.
- Enable and require TLS on UI↔Admin and plugin↔Admin channels, and mutual authentication so a plugin cannot be impersonated and policy cannot be MITM'd *(inferred)*.
- Configure and verify the upstream authentication layer (Kerberos) so the principal identities Ranger authorizes are genuine *(inferred)*.
- Rotate/secure any shipped default administrative credential before exposing the Admin *(inferred)*.
- Confirm the no-match default behavior (deny vs. fall-through to native ACLs) matches the intended posture for each service *(inferred)*.
- Secure the backing DB, audit sinks (Solr/HDFS), and KMS key store independently — Ranger trusts them *(inferred)*.
- Constrain delegated administrators' scope deliberately *(inferred)*.
- Understand that revocation propagates only at the policy-pull interval; size that interval to the data's sensitivity *(inferred)*.

---

## §11 Known misuse patterns

*(Hypothesized — confirm/expand.)*

- **Exposing the Admin REST API to an untrusted network.** Anyone reaching policy-write endpoints can grant themselves access to all guarded data *(inferred)*.
- **Running plugin↔Admin in plaintext** on a shared network, allowing policy interception or substitution *(inferred)*.
- **Treating Ranger as the sole gate while leaving a non-Ranger-consulting access path open** in the guarded service *(inferred)*.
- **Assuming instant revocation** rather than accounting for the pull interval and cache *(inferred)*.
- **Trusting tag-based policy without securing the tag source** (Atlas/tagsync) *(inferred)*.
- **Leaving the default admin account credentials in place** *(inferred)*.
- **Disabling audit and then relying on it for compliance evidence** *(inferred)*.

---

## §11a Known non-findings (recurring false positives)

*(To be populated by the PMC from real scanner/report history — this is a high-value section the team cannot fill from public sources alone.)*

Provisional candidates *(inferred)*:
- Reports against build/install/migration tooling or the ugsync utilities (`filesourceusersynctool`, `ldapconfigchecktool`) — out of scope per §3. *(Note: `ranger-examples/` and `ranger-tools/` are **in** scope per the PMC — do not file them as non-findings.)*
- "Plugin runs with the host service's privileges" — by design; the PEP is co-located inside the guarded service (§4). Not a privilege-escalation finding on its own.
- "Plugin serves stale policy when Admin is down" — documented availability behavior (§8 #2, FAQ), not a bug.
- "HTTP listener on 6080 / plaintext by default" flagged by a config scanner — plaintext is the supported default (§5a, §9), so this is `BY-DESIGN` / operator-hardening, not a code vulnerability *(maintainer, 2026-06-12 — §14 Q2)*.
- **Resource-name canonicalization mismatch** between the host service and the policy engine — a **shared responsibility** with the host service, not a Ranger-only defect *(maintainer, 2026-06-12 — §14 Q17)*.

**Correction (do NOT wave off):** a "default/seeded admin password" finding is **not** a non-finding — the installer mandates a complexity-checked password on fresh install, so a seeded/default password is not a supported posture (§5a) *(maintainer, 2026-06-12 — §14 Q3/Q18)*.

---

## §12 Conditions that would change this model

- A new plugin family or a new guarded service with a different trust profile.
- A change to the policy-distribution mechanism (e.g., push instead of pull, or a new transport).
- A change in the no-match default behavior or in the fail-open/fail-safe posture.
- A new authentication mode for Admin or plugins.
- Promotion of anything currently in `ranger-examples/` into core.
- A change to KMS key-store options or key-release authorization.
- **A vulnerability report that cannot be routed to a single §13 disposition** — this signals a model gap; revise §8/§9 rather than making an ad-hoc call.

---

## §13 Triage dispositions

| Disposition | Meaning | Licensed by |
| --- | --- | --- |
| `VALID` | Violates a claimed property (e.g., authorization bypass via policy-eval bug, unauthenticated policy write) by an in-scope adversary/input. | §8, §6, §7 |
| `VALID-HARDENING` | No §8 property broken, but Ranger elects to harden an easy misuse (e.g., tighten a default). Reported privately; CVE at maintainer discretion. | §11 |
| `OUT-OF-MODEL: trusted-input` | Requires control of an input the model marks trusted (e.g., the authenticated principal identity, or directly-tampered DB/key store). | §6 |
| `OUT-OF-MODEL: adversary-not-in-scope` | Requires an excluded capability (code execution on a plugin host; malicious top-level admin; side channel). | §7 |
| `OUT-OF-MODEL: unsupported-component` | Lands in `ranger-examples/` or build/install/migration tooling. | §3 |
| `OUT-OF-MODEL: non-default-build` | Only manifests under a discouraged/non-default config knob. | §5a |
| `BY-DESIGN: property-disclaimed` | Concerns a property §9 explicitly disclaims (no end-user auth, no transport security by default, stale-policy window, etc.). | §9 |
| `KNOWN-NON-FINDING` | Matches a documented recurring false positive. | §11a |
| `MODEL-GAP` | Cannot be cleanly routed — triggers a model revision. | §12 |

---

## §14 Open questions — RESOLVED by the Ranger PMC (2026-06-12)

The Apache Ranger PMC (**Abhishek Kumar, `@kumaab`**) reviewed this model on [apache/ranger#994](https://github.com/apache/ranger/pull/994) and answered all 20 open questions on **2026-06-12**. The answers are recorded below and folded into §1–§13; confirmed claims are promoted to *(maintainer, 2026-06-12)*. Abhishek volunteered to own ongoing revision (Q20).

**Wave 1 — scope & defaults**
1. **No-match default:** when Ranger is the ACL enforcer the result is **deny**, **except HDFS**, where it falls through to the host's native ACLs.
2. **Transport security default:** TLS is recommended in production; **plaintext HTTP is the shipped/supported default**.
3. **Default admin credential:** Ranger seeds accounts (`admin`, `rangerusersync`, `rangertagsync`, `keyadmin`) as install-bootstrap values, but the standard installer **requires an explicit, complexity-checked password on fresh install** (changeable any time via UI/REST). **A seeded/default password is not a supported production posture.**
4. **Scope:** **`ranger-examples` and `ranger-tools` are IN scope.** Out of model: `migration-util` and the ugsync utilities (`filesourceusersynctool`, `ldapconfigchecktool`).
5. **Caller roles:** the breakdown is correct; **add an Auditor role** (view policies + audits, no writes) and a **Key Admin role** (KMS only).

**Wave 2 — trust & authentication**
6. **Plugin↔Admin auth:** plugins and the PDP service (a *separate process*, not a plugin) authenticate via **Kerberos, JWT, or header-based auth with a trusted proxy** to download policy.
7. **End-user identity:** Ranger Admin authenticates **management/UI users** (LDAP, AD, Unix, PAM, Kerberos/SPNEGO, Knox SSO, header, JWT, internal DB). At **data-access time Ranger does not authenticate the end user** — the host service authenticates the principal and the PEP hands Ranger an already-authenticated principal.
8. **Byzantine plugin:** **full trust once authenticated** — a deliberate design choice that keeps runtime overhead low; **Ranger makes no cross-node integrity claim**.
9. **Fail mode:** if a plugin can neither evaluate fresh nor cached policy it **denies** — except the HDFS plugin, which can fall through to native ACLs.
10. **REST endpoint matrix:** three buckets — (1) **public** (login/static, `/service/actuator/health`, `/service/metrics/**`); (2) **plugin/PEP identity** (policy/tag/role/user/GDS download + grant/revoke — `security="none"` at Spring but app-layer authenticated by service client-cert or SPNEGO, with `secure/...download` variants requiring a user in `policy.download.auth.users`); (3) **authenticated admin/user** (everything else: policy/service/user/role/zone/audit CRUD, plugin info, KMS keys — session + `@PreAuthorize` RBAC, writes blocked for read-only Auditor).

**Wave 3 — properties, resources, KMS**
11. **Authorization-bypass severity:** an existing deny-policy (or absence of a grant) that nonetheless yields access is **CVE-class `VALID`**.
12. **Resource / DoS line:** "policy evaluation is bounded and thread-safe" is correct; **super-linear evaluation cost is NOT a bug**; a **hang inside the host service could be** a security issue; whether *no* resource guarantee is made is **left for later ratification**.
13. **KMS authorization:** the key is the KMS-policy resource; **unauthorized key retrieval is CVE-class `VALID`**.
14. **Audit integrity:** a non-admin able to **tamper audit records IS a security finding**.
15. **Stale-policy window:** revocation-propagation delay (pull interval + cache) is **by design**.

**Wave 4 — misuse, non-findings, ownership**
16. **Tagsync/usersync trust:** confirmed — policy is only as trustworthy as the tag/identity source; upstream injection is out of model.
17. **Resource canonicalization:** resource names must map correctly between service and policy — a **shared responsibility** with the host service.
18. **Known non-findings:** the §11a list is a good starting point and can evolve; **one correction — admin password is mandated at installation, so an insecure default password is NOT a supported posture** (folded into §5a/§11a).
19. **Side channels / co-tenancy:** **out of scope.**
20. **Ownership:** yes — this becomes the canonical Ranger security model; **Abhishek Kumar volunteers to own its revision.**

*Remaining genuinely-open item:* whether Ranger makes **no resource guarantee at all** (§8 #6) — the PMC deferred this for later ratification.
