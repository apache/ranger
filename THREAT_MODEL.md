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
- **Threat-model authors:** Drafted by the ASF Security team as a v0 pre-flight artifact for an automated agentic security scan. **Not yet reviewed by the Apache Ranger PMC.**
- **Date:** 2026-06-03
- **Status:** Draft — awaiting Ranger PMC review. Every position here is provisional until the PMC confirms, corrects, or strikes it (see §14).

**Version binding.** Once ratified, this model is intended to be versioned alongside Ranger and tagged with releases. A report against Ranger version *N* should be triaged against the model as it stood at *N*, not at HEAD. This v0 carries no such binding yet.

**Reporting cross-reference.** Apache Ranger has no `SECURITY.md` in the repository (confirmed 404) and no dedicated security/threat-model page on https://ranger.apache.org/ at the time of writing *(documented — absence verified)*. Vulnerabilities in Apache projects are reported to `security@ranger.apache.org` / `security@apache.org` per the ASF-wide process. Findings that fall under §8 (claimed properties) should be reported through that channel; findings that fall under §3 or §9 will be closed citing this document once it is ratified.

**Provenance legend.** Every non-trivial claim carries exactly one tag:
- *(documented)* — stated in Ranger's own docs (README, ranger.apache.org, FAQ, API docs) or directly verifiable from the public repository layout; cited inline.
- *(maintainer)* — stated by a Ranger maintainer in response to this process. **There are none yet** — this is v0 with no maintainer input.
- *(inferred)* — reasoned from code/repo structure, absence of a feature, or general domain knowledge; not yet confirmed. Every *(inferred)* claim has a matching question in §14.

**Draft confidence:** 18 documented / 0 maintainer / 47 inferred. The model is overwhelmingly hypothesis at this stage; the §14 questions are the path to promoting *(inferred)* claims to *(maintainer)*.

**What Apache Ranger is.** Apache Ranger is "a framework to enable, monitor and manage comprehensive data security across 20+ data processing services" *(documented — ranger.apache.org)*. It provides centralized & fine-grained authorization, audit, and key management for data services (Trino, Polaris, Ozone, HDFS, Hive, HBase, Kafka, NiFi, Knox, Kudu, YARN, Solr, Atlas and others). The administrator defines access-control policies in a central web application (Ranger Admin); lightweight per-service Java plugins, deployed inside each data service's own process, pull those policies and enforce authorization decisions locally on every access request. In threat-model terms Ranger is a **distributed Policy Decision / Policy Enforcement system**: the policy decision authority lives with the admin server (which authors and distributes policy), but the actual *enforcement* (the PEP) runs as plugin code co-located inside the data service it guards *(documented — FAQ: plugins "run as part of the same process as the namenode (HDFS), Hive2Server(Hive), HBase server (Hbase)")*.

---

## §2 Scope and intended use

**Primary intended use.** Centralized authoring of fine-grained authorization policies (resource-based and tag-based) and their enforcement across many independent data services, plus unified audit of access decisions and a key-management service (Ranger KMS) for HDFS transparent encryption *(documented — ranger.apache.org goals: "Centralized security administration", "Fine grained authorization", "Centralize auditing of user access")*.

**Deployment context.** Ranger is **not** an in-process library. It is a multi-component distributed system deployed across a cluster: a central web application/daemon (Ranger Admin, default port 6080 *(documented — README)*), a user/group sync daemon, an optional tag-sync daemon, the Ranger KMS daemon, and a fleet of plugins each embedded inside a separate data-service process on separate (typically trusted, intra-cluster) hosts *(documented — README lists Admin Tool, User Synchronization, and component plugins as the deployment tiers)*.

**Caller roles.** Because Ranger is a distributed service rather than a single library, "the caller" splits into distinct roles, each modeled separately in §6/§7:

- **Security administrator** — authors policies, manages users/roles, views audits via the Admin UI or REST API. Trusted for the instance.
- **Delegated administrator** — a group owner to whom administration of a subset of resources has been delegated *(documented — FAQ: "delegate administration of certain data to other group owners")*. Trusted for their delegated scope only.
- **Deployed plugin (PEP)** — code running inside a data service, authenticated to the Admin server, that downloads policy and enforces it. Trusted to enforce honestly *(inferred)*.
- **End user of the guarded data service** — the principal whose HDFS/Hive/etc. access Ranger authorizes. **Untrusted** with respect to the decisions Ranger makes about them; they do not interact with Ranger directly but their identity and requested action are the inputs to the PEP *(inferred)*.
- **Identity source** — LDAP/AD/Unix that usersync reads from. Trusted as the authority for user/group membership *(inferred)*.

**Component-family table.**

| Family | Representative entry point | Touches outside its process | In this model? |
| --- | --- | --- | --- |
| Ranger Admin web app (PDP + admin) | `security-admin/`, REST `/service/...`, `/public/v2/api/...` *(documented — API docs)* | DB, network (serves plugins + UI), LDAP/AD | **In** |
| Per-service plugins (PEPs) | `hdfs-agent/`, `hive-agent/`, `hbase-agent/`, `plugin-kafka/`, `plugin-*`, `agents-common/`, `pdp/` | Embedded in host service; network to Admin; audit sink | **In** |
| Plugin shims / classloader | `ranger-*-plugin-shim/`, `ranger-plugin-classloader/` | Class loading inside host service | **In** (as part of PEP delivery) |
| User/group sync | `ugsync/`, `ugsync-util/`, `unixauth*/` | LDAP/AD/Unix; writes to Admin DB | **In** |
| Tag sync | `tagsync/` | Atlas/Kafka; writes to Admin | **In** |
| Audit framework | `agents-audit/`, `audit-server/` | Solr/OpenSearch/Kafka/HDFS/DB audit sinks | **In** |
| Ranger KMS | `kms/`, `plugin-kms/` | Key store (DB/HSM), HDFS NameNode/DataNode | **In** (model at its own trust level — handles key material) |
| Authentication | `ranger-authn/`, `unixauthservice/`, `agents-cred/`, `credentialbuilder/` | Kerberos/SPNEGO/LDAP; credential stores | **In** |
| Examples / sample app | `ranger-examples/` (`sampleapp`, `plugin-sampleapp`, `sample-client`) *(documented — repo layout)* | Demo only | **Out** — see §3 |
| Build / install / migration tooling | `distro/`, `agents-installer/`, `migration-util/`, `dev-support/`, `build_ranger_using_docker.sh` | Build/deploy host | **Out** — see §3 |

---

## §3 Out of scope (explicit non-goals)

- **`ranger-examples/` (sample app, plugin-sampleapp, sample-client).** Shipped as demonstration/integration scaffolding *(documented — repo layout)*. Treated as unsupported for security purposes; a finding here is `OUT-OF-MODEL: unsupported-component` *(inferred)*.
- **Build, packaging, install, and migration tooling** (`distro/`, `agents-installer/`, `migration-util/`, Docker build scripts). SDLC/deployment hygiene, not the runtime trust surface *(inferred)*.
- **The guarded data services themselves.** Ranger authorizes access *within* HDFS, Hive, HBase, etc., but it does not own those services' own attack surface (e.g., an HDFS RPC bug). Ranger's responsibility begins at the authorization decision and ends at returning allow/deny to the host service *(inferred)*.
- **Authentication of end users.** Ranger authorizes an *already-authenticated* principal; establishing that the principal is who they claim (Kerberos ticket validation, etc.) is performed by the host data service / cluster Kerberos infrastructure, not by Ranger's PDP. Ranger trusts the identity the PEP presents *(inferred)*.
- **Network transport security as a Ranger guarantee.** Whether plugin↔Admin and UI↔Admin traffic is TLS-protected is a deployment configuration concern; Ranger supports it but does not enforce it at the protocol layer *(inferred)*.
- **Correctness/security of third-party identity stores** (LDAP/AD) and audit/key backends (Solr, HDFS, RDBMS, HSM). Ranger integrates with them but does not model their internal threats *(inferred)*.
- **Defense against a fully-compromised host running a plugin.** A plugin runs inside the data-service process on a cluster node; an attacker with code execution on that node has already defeated the PEP locally (see §7) *(inferred)*.

---

## §4 Trust boundaries and data flow

**Where the boundary sits.** Ranger has several trust boundaries, not one:

1. **End user ↔ guarded data service (the access request).** The user's identity + requested resource/action enters the host service, which calls the embedded Ranger plugin (PEP). This is the primary *untrusted-input* boundary: the requested resource name and action are derived from an untrusted user, though the *identity* is asserted by the host service's authentication layer, which Ranger trusts *(inferred)*.

2. **Plugin (PEP) ↔ Ranger Admin (policy distribution).** Plugins "pull the policy-changes using REST API at a configured regular interval (e.g.: 30 second)" *(documented — FAQ)*. The plugin trusts that the policy it downloads genuinely originates from the legitimate Admin server, and the Admin trusts that only authorized plugins download policy. This boundary is authenticated *(inferred — mechanism, likely Kerberos/SPNEGO or service credentials, to be confirmed)*. Plugins cache the last-known policy and "function even if the policy server is temporarily down" *(documented — FAQ)*, so the cache is load-bearing.

3. **Admin UI / REST client ↔ Ranger Admin (administration).** Administrators author policy and read audits over `/service/...` and `/public/v2/api/...` REST endpoints *(documented — API docs)*. This is the highest-value boundary: anyone who can author policy can grant themselves or others access to all guarded data.

4. **Usersync/tagsync ↔ Admin, and Admin ↔ DB / audit sink / key store.** Internal data-plane boundaries; integrity of the user/group/role data and the policy store determines the integrity of every downstream decision *(inferred)*.

**Data flow (authorization path).** Untrusted user request → host service authenticates principal → host service calls embedded Ranger plugin with (principal, resource, action) → plugin evaluates locally-cached policy (resource-based + tag-based) → returns allow/deny → plugin emits an audit record to the configured sink → host service enforces the decision *(documented in part — FAQ; full flow inferred)*.

**Reachability preconditions per component (the triager's first test):**

- A finding in a **plugin / `pdp/` / `agents-common`** is in-model only if reachable from a policy-evaluation input — i.e., from the (principal, resource, action) tuple of an untrusted access request, or from the downloaded policy document, or from the plugin's local config *(inferred)*.
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

- **TLS on plugin↔Admin and UI↔Admin.** Default and whether HTTP (non-TLS) is a supported production posture is unconfirmed *(inferred — wave-1 question)*. If the default is plaintext, policy in transit is exposed; this is the canonical "insecure-default" case the rubric flags.
- **Authentication mode for the Admin** (Kerberos/SPNEGO vs. LDAP/AD vs. Unix vs. local "admin" account). The shipped default admin credential and whether it must be rotated before exposure is unconfirmed *(inferred)*.
- **Audit destination** (Solr / HDFS / DB / log file / none). Disabling audit voids the §8 auditability property *(inferred)*.
- **`deny`-by-default vs. fallback-to-native-ACL** when no Ranger policy matches a request. This is a security-defining knob and its default must be pinned *(inferred — wave-1 question)*.
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
- **Side-channel / timing adversaries** against the policy engine or KMS *(inferred)*.

**Authenticated-but-Byzantine participant.** Ranger is a distributed PDP/PEP system: a plugin holds a legitimate service identity and could, if its host is compromised, behave arbitrarily (return wrong decisions, withhold audit). Whether Ranger makes any cross-node integrity claim under a misbehaving-but-authenticated plugin, or treats every PEP as fully trusted once authenticated, must be stated *(inferred — see §14)*.

---

## §8 Security properties the project provides

*(All entries below are hypothesized for the PMC to confirm; none are maintainer-ratified yet.)*

1. **Authorization decisions reflect the authored policy.** Given correctly-distributed policy and a correctly-authenticated principal, a plugin's allow/deny matches what the central policy specifies for that (principal, resource, action). *Violation symptom:* a user is granted access the policy denies (or denied access the policy grants) — i.e., a policy-evaluation/matching bug. *Severity:* security-critical (authorization bypass → CVE-class). *(inferred)*

2. **Centralized policy is faithfully distributed to enforcement points.** Policy authored in Admin is delivered to plugins via the periodic pull and applied; plugins fall back to last-cached policy when Admin is unreachable rather than failing open arbitrarily. *Violation symptom:* a plugin enforces stale or wrong policy in a way that grants access the current policy denies; or distribution lets an attacker substitute policy. *Severity:* security-critical. *(documented in part — FAQ on pull interval + cache fallback; the "faithfully/integrity" guarantee is inferred)*

3. **Administrative actions are access-controlled.** Authoring/modifying policy, managing users/roles, and reading audits via the REST API and UI require appropriate authenticated, authorized identity; delegated admins are confined to their delegated scope. *Violation symptom:* an unauthenticated or under-privileged actor authors/reads policy, escalates a role, or reads audit/keys. *Severity:* security-critical. *(documented in part — delegation in FAQ; enforcement details inferred)*

4. **Access decisions are audited.** Allow/deny decisions and administrative actions are recorded to the configured audit sink. *Violation symptom:* an in-scope access produces no audit record, or audit can be silently suppressed/forged by a non-admin. *Severity:* security-relevant (integrity of the audit trail); often correctness-only for missed records, critical for forgeable records. *(documented — ranger.apache.org: "Centralize auditing of user access and administrative actions")*

5. **KMS authorizes key access per policy.** Ranger KMS releases key material only to clients authorized by key-access policy. *Violation symptom:* unauthorized key retrieval / decryption capability. *Severity:* security-critical. *(inferred)*

6. **Policy evaluation is bounded and thread-safe.** Each authorization call terminates promptly and concurrent evaluation during a policy refresh does not corrupt decisions. *Violation symptom:* hang/CPU exhaustion inside the host service on crafted resource input, or a race producing a wrong decision. *Severity:* availability-critical (a hung PEP can stall the host service) for DoS; critical for the race. *Threshold:* to be set — is super-linear evaluation cost in policy size or resource-string length a bug? *(inferred — §14)*

---

## §9 Security properties the project does *not* provide

*(Hypothesized; confirm/correct.)*

- **No authentication of end users.** Ranger authorizes an already-authenticated principal; it does not establish identity. A forged/spoofed principal accepted by the host service's authentication layer is the host service's / cluster Kerberos's problem, not Ranger's *(inferred)*.
- **No protection against a malicious top-level administrator.** The admin is the root of trust for the instance *(inferred)*.
- **No defense of a plugin against its own compromised host.** A PEP running in a compromised data-service process can be bypassed locally *(inferred)*.
- **No transport security guarantee by default (to confirm).** Ranger supports TLS but does not enforce it at the protocol layer; plaintext deployment exposes policy and audit in transit *(inferred)*.
- **No guarantee about the guarded service's own attack surface.** Ranger only renders authorization decisions *(inferred)*.

**False friends (highest-value warnings — confirm):**
- **A Ranger "deny" is not a sandbox.** Enforcement depends on the host service actually calling the plugin for every access path. A code path in the guarded service that does not consult the plugin is not protected by Ranger *(inferred)*.
- **Cached policy is availability, not authority.** The "works when Admin is down" cache *(documented — FAQ)* means a plugin can enforce *stale* policy: a just-revoked grant may persist until the next successful pull. Operators must not treat revocation as instantaneous *(inferred)*.
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
- Reports against `ranger-examples/` (sampleapp, sample-client) — out of scope per §3.
- "Plugin runs with the host service's privileges" — by design; the PEP is co-located inside the guarded service (§4). Not a privilege-escalation finding on its own.
- "Plugin serves stale policy when Admin is down" — documented availability behavior (§8 #2, FAQ), not a bug.
- "Default admin password" / "HTTP listener on 6080" flagged by a config scanner — an operator-hardening item (§10), `VALID-HARDENING` at most, not a code vulnerability — *pending PMC ruling on whether the insecure default is supported posture*.

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

## §14 Open questions for the maintainers

Every *(inferred)* claim above routes to a question below. Each states a proposed answer to confirm, correct, or strike. Grouped in waves.

**Wave 1 — Scope, defaults, and the decisions that reshape everything**
1. **No-match default:** When no Ranger policy matches a request, does the plugin **deny**, or fall through to the host service's native ACLs? We assume it depends on per-service config; what is the shipped default? (→ §5a, §8 #2, §11a)
2. **Transport security default:** Is TLS on plugin↔Admin and UI↔Admin required for a supported production deployment, or is plaintext a supported posture? (→ §5a, §9, §13 `non-default-build` vs `VALID`)
3. **Default admin credential:** Does Ranger ship a default admin account/password, and is rotating it a documented operator requirement before exposure? (→ §5a, §10, §11a)
4. **Scope confirmation:** Are `ranger-examples/` and the install/migration tooling correctly out of model? Any other shipped-but-unsupported code we missed? (→ §2, §3)
5. **Caller roles:** Is the five-role breakdown in §2 (admin / delegated admin / plugin / end user / identity source) correct and complete? (→ §2, §7)

**Wave 2 — Trust boundaries and authentication**
6. **Plugin↔Admin auth mechanism:** How does a plugin authenticate to Admin to download policy, and how does it verify the policy's origin (Kerberos/SPNEGO? service credentials? signed policy)? (→ §4, §6)
7. **End-user identity trust:** Do we correctly model Ranger as trusting the principal identity asserted by the host service (Kerberos), and *not* performing user authentication itself? (→ §3, §9)
8. **Byzantine plugin:** Once authenticated, is a plugin fully trusted, with no cross-node integrity claim if its host is compromised? (→ §7)
9. **Fail mode on total evaluation failure:** If a plugin can neither evaluate fresh nor cached policy, does it deny, or defer to the host service? (→ §5, §8 #2)
10. **REST endpoint auth matrix:** Which `/service/...` and `/public/v2/api/...` endpoints are reachable unauthenticated vs. require admin vs. require a plugin identity? (→ §6, §8 #3)

**Wave 3 — Properties, resources, and KMS**
11. **Authorization-bypass severity:** Confirm a policy-evaluation/matching bug that grants denied access is CVE-class `VALID`. (→ §8 #1, §13)
12. **Resource bound / DoS line:** Is super-linear policy-evaluation cost (in policy count or resource-string length) a bug? Is a hang inside the host service a security issue? Or is no resource guarantee made? (→ §8 #6)
13. **KMS authorization:** Confirm Ranger KMS releases key material only per key-access policy, and that unauthorized key retrieval is `VALID`/CVE-class. (→ §8 #5)
14. **Audit integrity:** Is a forgeable/suppressible audit record (by a non-admin) a security finding, or is audit best-effort? (→ §8 #4, §9)
15. **Stale-policy window as disclaimed:** Confirm that the revocation-propagation delay (pull interval + cache) is by-design, not a bug. (→ §9 false friends, §11a)

**Wave 4 — Misuse, non-findings, and ownership**
16. **Tagsync/usersync trust:** Confirm policy is only as trustworthy as the tag/identity source, and injection upstream of Ranger is out of model (`trusted-input`). (→ §6, §9, §11)
17. **Resource canonicalization:** Is mismatch between host-service resource naming and policy-engine matching (path normalization, case, wildcards) a known bypass class you actively defend, or a shared responsibility with the host service? (→ §9, §11)
18. **Known non-findings:** What do scanners/researchers most often report against Ranger that you consider a non-finding, and why? (→ §11a — needs maintainer history)
19. **Side channels / co-tenancy:** Confirm timing/side-channel adversaries against the policy engine and KMS are out of scope. (→ §7)
20. **Document ownership & coexistence:** Since Ranger has no `SECURITY.md`, should this become the canonical security model linked from the project site, and who on the PMC owns its revision? (→ §1; meta)
