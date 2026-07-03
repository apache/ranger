# Tag Sync — Atlas Kafka configuration (Kafka 3.9+ / KRaft)

Reference for operators and developers configuring Ranger Tag Sync with the **Atlas Kafka** tag source (`ranger.tagsync.source.atlas`).

Related Jira: [RANGER-5658](https://issues.apache.org/jira/browse/RANGER-5658) — remove obsolete `atlas.kafka.zookeeper.connect` and fix Atlas Kafka consumer setup for PLAINTEXT and Kerberos brokers.

---

## Summary

Tag Sync consumes Atlas entity notifications on the **`ATLAS_ENTITIES`** topic using Atlas's `KafkaNotification` / standard **`kafka-clients`** (`bootstrap.servers`, consumer group, security protocol). It does **not** connect to Kafka via ZooKeeper.

The property `atlas.kafka.zookeeper.connect` was legacy configuration. Ranger previously **required** it at startup even though Atlas 2.4 `KafkaNotification` never uses it for consumer creation, and Kafka 3.x / KRaft clients use `bootstrap.servers` only. That requirement has been **removed**.

**Both PLAINTEXT and Kerberos (SASL) Atlas Kafka are supported.** Set `TAG_SOURCE_ATLAS_KAFKA_SECURITY_PROTOCOL` to match how Atlas publishes entity notifications.

---

## Supported Atlas Kafka security modes

| Mode | `TAG_SOURCE_ATLAS_KAFKA_SECURITY_PROTOCOL` | Extra install properties | `setup.py` writes Kerberos JAAS? |
|------|---------------------------------------------|--------------------------|----------------------------------|
| **PLAINTEXT** | `PLAINTEXT` | Bootstrap + consumer group only | **No** |
| **Kerberos** | `SASL_PLAINTEXT` or `SASL_SSL` | `TAG_SOURCE_ATLAS_KAFKA_SERVICE_NAME`, `TAG_SOURCE_ATLAS_KERBEROS_PRINCIPAL`, `TAG_SOURCE_ATLAS_KERBEROS_KEYTAB` | **Yes** (when `is_secure=true`) |

`setup.py` uses `atlas_kafka_uses_kerberos()` — Kerberos JAAS for the Atlas Kafka client is emitted **only** when the security protocol starts with `SASL`. This allows Ranger to run with Kerberos (`is_secure=true`) while consuming from a **PLAINTEXT** Atlas metadata bus (common in Atlas+Ranger coexist / dev stacks).

**Important:** Atlas Kafka security is independent of how Tag Sync authenticates to **Ranger Admin** for tag upload (basic auth or Ranger Kerberos via `core-site.xml` / jceks). Configure each path separately.

---

## Required configuration (Atlas Kafka source)

Set these in `install.properties` (installer writes `conf/atlas-application.properties`):

| Install property | Atlas property | Required |
|------------------|----------------|----------|
| `TAG_SOURCE_ATLAS_ENABLED` | (enables `ranger.tagsync.source.atlas` in site.xml) | Yes, for Kafka source |
| `TAG_SOURCE_ATLAS_KAFKA_BOOTSTRAP_SERVERS` | `atlas.kafka.bootstrap.servers` | Yes |
| `TAG_SOURCE_ATLAS_KAFKA_ENTITIES_GROUP_ID` | `atlas.kafka.entities.group.id` | Yes |
| `TAG_SOURCE_ATLAS_KAFKA_SECURITY_PROTOCOL` | `atlas.kafka.security.protocol` | Yes (defaults to PLAINTEXT if omitted in logic) |
| `TAG_SOURCE_ATLAS_KAFKA_SERVICE_NAME` | `atlas.kafka.sasl.kerberos.service.name` | If Kerberos |
| `TAG_SOURCE_ATLAS_KERBEROS_PRINCIPAL` / `KEYTAB` | `atlas.jaas.KafkaClient.*` | If Kerberos |

### Example — PLAINTEXT (Atlas metadata bus / KRaft coexist)

Use when Atlas publishes `ATLAS_ENTITIES` on a **separate PLAINTEXT** metadata Kafka (e.g. `atlas-kafka` in Atlas+Ranger coexist stacks).

`install.properties`:

```properties
TAG_SOURCE_ATLAS_ENABLED=true
TAG_SOURCE_ATLAS_KAFKA_BOOTSTRAP_SERVERS = atlas-kafka.example.com:9092
TAG_SOURCE_ATLAS_KAFKA_ENTITIES_GROUP_ID = ranger_entities_consumer
TAG_SOURCE_ATLAS_KAFKA_SECURITY_PROTOCOL = PLAINTEXT
TAG_SOURCE_ATLASREST_ENABLED=false
TAG_SOURCE_FILE_ENABLED=false
```

Generated `conf/atlas-application.properties`:

```properties
atlas.kafka.bootstrap.servers=atlas-kafka.example.com:9092
atlas.kafka.entities.group.id=ranger_entities_consumer
atlas.kafka.security.protocol=PLAINTEXT
```

### Example — Kerberos (secured Atlas Kafka)

Use when Atlas entity notifications are on a **SASL** Kafka broker. In **ranger-docker**, that is typically `ranger-kafka.rangernw:9092` (not PLAINTEXT).

`install.properties`:

```properties
TAG_SOURCE_ATLAS_ENABLED=true
TAG_SOURCE_ATLAS_KAFKA_BOOTSTRAP_SERVERS = ranger-kafka.rangernw:9092
TAG_SOURCE_ATLAS_KAFKA_ENTITIES_GROUP_ID = ranger_entities_consumer
TAG_SOURCE_ATLAS_KAFKA_SECURITY_PROTOCOL = SASL_PLAINTEXT
TAG_SOURCE_ATLAS_KAFKA_SERVICE_NAME = kafka
TAG_SOURCE_ATLAS_KERBEROS_PRINCIPAL = rangertagsync/ranger-tagsync.rangernw@EXAMPLE.COM
TAG_SOURCE_ATLAS_KERBEROS_KEYTAB = /etc/keytabs/rangertagsync.keytab
```

Requires `is_secure=true` in install so Kerberos JAAS is written for the Atlas Kafka client.

See also: `dev-support/ranger-docker/scripts/tagsync/ranger-tagsync-install.properties`.

---

## Starting Tag Sync

### Native / tarball install

After `setup.sh` in the tagsync package:

```bash
cd ${RANGER_TAGSYNC_HOME}/tagsync
./ranger-tagsync-services.sh start    # stop | restart
```

Or via the init wrapper (runs as `ranger` user):

```bash
/usr/bin/ranger-tagsync.sh start      # stop | restart | status
```

### JVM flag (Atlas Kafka source)

`ranger-tagsync-services.sh` must pass the install conf directory as Atlas config root:

```bash
-Datlas.conf="${TAGSYNC_CONF_DIR}"
```

Without this, `AtlasTagSource` may not load installer-generated `atlas-application.properties` and Kafka consumer startup will fail or use wrong settings. Custom systemd/k8s wrappers must include the same flag.

### Docker (ranger-docker)

Default sample (`ranger-tagsync-install.properties`): **file source on** (no Atlas server in ranger-docker), **Atlas REST off**, **Atlas Kafka off**. For Atlas+Ranger coexist stacks, set `TAG_SOURCE_ATLAS_ENABLED=true`, `TAG_SOURCE_ATLASREST_ENABLED=false`, and `TAG_SOURCE_FILE_ENABLED=false` to consume `ATLAS_ENTITIES` from Kafka.

```bash
cd dev-support/ranger-docker
docker compose -f docker-compose.ranger.yml -f docker-compose.ranger-tagsync.yml up -d
```

The container entrypoint runs `setup.sh` then `ranger-tagsync-services.sh start`.

After changing `install.properties` or Atlas Kafka settings, force re-setup:

```bash
docker exec ranger-tagsync rm -f /opt/ranger/ranger-*-tagsync/.setupDone
docker restart ranger-tagsync
```

### Verify Tag Sync is running

```bash
# Process
ps -ef | grep TagSynchronizer

# Atlas Kafka config on disk
grep -E 'bootstrap.servers|security.protocol|zookeeper' \
  /etc/ranger/tagsync/conf/atlas-application.properties

# Logs
tail -f /var/log/ranger/tagsync/tagsync-*.log
```

Expect: bootstrap servers and consumer group set; **no** `atlas.kafka.zookeeper.connect`; for PLAINTEXT, **no** `atlas.jaas.KafkaClient` entries.

---

## Removed configuration

| Property | Status |
|----------|--------|
| `atlas.kafka.zookeeper.connect` | **No longer required** — not generated by `setup.py`, not validated by `AtlasTagSource` |

Existing deployments may still have this key in `atlas-application.properties`; it is harmless and can be removed on the next config refresh.

---

## ZooKeeper still used elsewhere (do not confuse)

| Config | Purpose | Needed for KRaft Kafka? |
|--------|---------|-------------------------|
| `atlas.kafka.zookeeper.connect` | Legacy Atlas/Kafka config | **No** (removed) |
| `ranger-tagsync.server.ha.zookeeper.connect` | Tag Sync active/passive leader election | **Only when HA enabled** |

---

## Alternative: Atlas REST source

If Tag Sync reads tags over Atlas REST instead of Kafka, Atlas Kafka properties do not apply:

```properties
TAG_SOURCE_ATLASREST_ENABLED=true
TAG_SOURCE_ATLASREST_ENDPOINT = http://localhost:21000
TAG_SOURCE_ATLAS_ENABLED=false
```

Enable via `ranger.tagsync.source.atlasrest` in `ranger-tagsync-site.xml`.

---

## Test plan

- [ ] Fresh install: `atlas-application.properties` has no `zookeeper.connect`
- [ ] PLAINTEXT: `is_secure=true` + `security.protocol=PLAINTEXT` → no Kerberos JAAS in atlas props
- [ ] SASL: Kerberos JAAS written when `security.protocol=SASL_PLAINTEXT`
- [ ] Tag Sync starts with `-Datlas.conf` and consumes `ATLAS_ENTITIES` from Kafka 3.9.x (KRaft)
- [ ] Kerberos Atlas Kafka: consumer connects with principal/keytab
- [ ] Tag Sync HA smoke test (if `ranger-tagsync.server.ha.enabled=true`)

---

## References

- Tag Sync Atlas source: `tagsync/src/main/java/org/apache/ranger/tagsync/source/atlas/AtlasTagSource.java`
- Atlas notification: `org.apache.atlas.kafka.KafkaNotification` in `atlas-notification`
- Docker sample: `dev-support/ranger-docker/scripts/tagsync/ranger-tagsync-install.properties`
- Ranger docker overview: `dev-support/ranger-docker/README.md`
