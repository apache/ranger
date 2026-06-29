# RANGER-5658 — Remove obsolete atlas.kafka.zookeeper.connect from Tag Sync

Single reference for the Jira ticket, GitHub PR, and reviewers.

**Jira:** [RANGER-5658](https://issues.apache.org/jira/browse/RANGER-5658)  
**Branch:** `RANGER-5658-patch`

| Field | Value |
|-------|--------|
| **PR title** | `RANGER-5658: Remove obsolete atlas.kafka.zookeeper.connect requirement from Tag Sync` |
| **Copy target** | Paste [Jira / PR description](#jira--pr-description-copy-from-here) below into Jira **Description** and/or GitHub PR body |

---

## Jira / PR description (copy from here)

Remove the legacy `atlas.kafka.zookeeper.connect` configuration requirement from Ranger Tag Sync's Atlas Kafka source.

Tag Sync consumes Atlas entity notifications via `kafka-clients` using `atlas.kafka.bootstrap.servers` and `atlas.kafka.entities.group.id`. The `zookeeper.connect` property was never used by Atlas 2.4 `KafkaNotification` for consumer creation; Ranger only validated that it was set. With Kafka 3.9.x and KRaft brokers, clients do not use ZooKeeper for consumption.

### Changes

- Drop `TAGSYNC_ATLAS_ZOOKEEPER_ENDPOINT` constant and startup validation from `AtlasTagSource`
- Extract `validateRequiredAtlasKafkaProperties()` for unit testing
- Remove `TAG_SOURCE_ATLAS_KAFKA_ZOOKEEPER_CONNECT` from `install.properties`, `installprop2xml.properties`, and `setup.py`

### Out of scope

- Tag Sync HA (`ranger-tagsync.server.ha.zookeeper.*`) — unchanged; still requires ZooKeeper when HA is enabled
- Atlas REST tag source — unaffected

### Upgrade note

Existing deployments may still have `atlas.kafka.zookeeper.connect` in `conf/atlas-application.properties`; it is harmless and can be removed manually. New installs no longer generate or require it.

---

## Test plan

### Unit tests

```bash
mvn test -pl tagsync -Drat.skip=true \
  -Dtest=AtlasTagSourceConfigTest \
  -Dsurefire.failIfNoSpecifiedTests=false
```

| Test | Verifies |
|------|----------|
| `validateRequiredAtlasKafkaProperties_acceptsBootstrapAndGroupWithoutZookeeper` | No ZK property required when bootstrap + group are set |
| `validateRequiredAtlasKafkaProperties_rejectsMissingBootstrapServers` | Bootstrap servers still mandatory |
| `validateRequiredAtlasKafkaProperties_rejectsMissingConsumerGroup` | Consumer group still mandatory |

### Manual / integration (recommended)

- [ ] Fresh Tag Sync install with Atlas Kafka source — `conf/atlas-application.properties` has no `zookeeper.connect`
- [ ] Tag Sync starts and consumes Atlas entity notifications from Kafka 3.9.x (KRaft)
- [ ] Upgrade: existing install with legacy `zookeeper.connect` still starts
- [ ] Tag Sync HA smoke test (if `ranger-tagsync.server.ha.enabled=true`) — leader election unchanged

---

## Files changed

| File | Change |
|------|--------|
| `tagsync/src/main/java/.../AtlasTagSource.java` | Remove ZK validation; add `validateRequiredAtlasKafkaProperties()` |
| `tagsync/src/test/java/.../AtlasTagSourceConfigTest.java` | Unit tests for required Kafka properties |
| `tagsync/scripts/install.properties` | Remove `TAG_SOURCE_ATLAS_KAFKA_ZOOKEEPER_CONNECT` |
| `tagsync/conf/templates/installprop2xml.properties` | Remove install → Atlas mapping |
| `tagsync/scripts/setup.py` | Stop writing `atlas.kafka.zookeeper.connect` |
