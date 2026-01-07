# Ranger Audit HA Load Testing Scripts

This directory contains scripts for simulating High Availability (HA) load scenarios with Ranger Audit Server, HDFS, and Hive plugins.

## Quick Start

### 1. Start HA Audit Servers with HAProxy

```bash
cd /Users/rmani/git/ranger-audit-test/ranger/dev-support/ranger-docker/scripts/load-testing
chmod +x *.sh

# Start 3 audit servers with HAProxy load balancer
./start_ha_audit_servers.sh 3
```

### 2. Run Complete HA Load Test

This runs a comprehensive test with multiple phases:
- Baseline load (3 servers)
- Scale-up test (3 → 5 servers)
- Failure simulation (1 server down)
- Scale-down test (4 → 2 servers)

```bash
# Run test: duration=5min, hdfs_ops=1000, hive_ops=100, concurrency=10
./run_ha_load_test.sh 5 1000 100 10
```

### 3. Generate Individual Loads

**HDFS Load:**
```bash
# Copy script to hadoop container and run
docker cp hdfs_load_generator.sh ranger-hadoop:/tmp/
docker exec ranger-hadoop /tmp/hdfs_load_generator.sh 1000 10
# Args: num_operations=1000, concurrency=10
```

**Hive Load:**
```bash
# Copy script to hive container and run
docker cp hive_load_generator.sh ranger-hive:/tmp/
docker exec ranger-hive /tmp/hive_load_generator.sh 100
# Args: num_operations=100
```

### 4. Monitor HA Load Distribution

```bash
# Monitor in real-time (refresh every 5 seconds)
./monitor_ha_load.sh 5
```

## Scripts Overview

### `start_ha_audit_servers.sh`
- **Purpose**: Start Ranger Audit Servers in HA mode with HAProxy
- **Usage**: `./start_ha_audit_servers.sh [num_servers]`
- **Default**: 3 servers
- **Example**: `./start_ha_audit_servers.sh 5` (start 5 servers)

### `hdfs_load_generator.sh`
- **Purpose**: Generate HDFS operations to create audit events
- **Usage**: `./hdfs_load_generator.sh [num_operations] [concurrency]`
- **Default**: 1000 operations, 10 concurrent threads
- **Operations**: mkdir, put, cat, stat, ls, chmod, chown
- **Audit Events**: ~5 per operation

### `hive_load_generator.sh`
- **Purpose**: Generate Hive SQL operations to create audit events
- **Usage**: `./hive_load_generator.sh [num_operations]`
- **Default**: 100 insert operations
- **Operations**: CREATE DB/TABLE, INSERT, SELECT, ALTER, DROP
- **Audit Events**: ~3 per operation

### `monitor_ha_load.sh`
- **Purpose**: Real-time monitoring of HA load distribution
- **Usage**: `./monitor_ha_load.sh [refresh_interval_seconds]`
- **Default**: 5 second refresh
- **Displays**: 
  - HAProxy status
  - Audit server resource usage
  - Kafka consumer groups
  - Recent logs

### `run_ha_load_test.sh`
- **Purpose**: Comprehensive HA test orchestration
- **Usage**: `./run_ha_load_test.sh [duration_min] [hdfs_ops] [hive_ops] [concurrency]`
- **Default**: 5 minutes, 1000 HDFS ops, 100 Hive ops, 10 concurrency
- **Test Phases**:
  1. Baseline load with 3 servers
  2. Scale up to 5 servers under load
  3. Simulate server failure
  4. Scale down to 2 servers

## Manual Operations

### Scale Audit Servers

```bash
cd /Users/rmani/git/ranger-audit-test/ranger/dev-support/ranger-docker

# Scale up to 5 servers
docker compose -f docker-compose.ranger-audit-haproxy.yml up -d --scale ranger-audit-svc=5

# Scale down to 2 servers
docker compose -f docker-compose.ranger-audit-haproxy.yml up -d --scale ranger-audit-svc=2
```

### Check HAProxy Load Balancing

```bash
# View HAProxy logs
docker logs -f ranger-audit

# Test round-robin distribution
for i in {1..10}; do curl -s http://localhost:7081/ | head -1; done
```

### Monitor Kafka Partitions

```bash
# Check topic details
docker exec ranger-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe --topic ranger_audits

# Check consumer groups
docker exec ranger-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --list

# Check consumer lag
docker exec ranger-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group ranger_audit_solr_consumer_group \
  --describe
```

### View Audit Events

```bash
# View recent Kafka messages
docker exec ranger-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic ranger_audits \
  --max-messages 10

# Check Solr audit records (if enabled)
docker exec ranger-solr curl "http://localhost:8983/solr/ranger_audits/select?q=*:*&rows=10&sort=event_time desc"

# Check HDFS audit files (if enabled)
docker exec ranger-hadoop hdfs dfs -ls -R /ranger/audit/
```

## Failure Scenarios

### Simulate Server Failure

```bash
# Stop one audit server
docker ps | grep ranger-audit-svc
docker stop <container_id_or_name>

# Generate load - HAProxy routes to remaining servers
docker exec ranger-hadoop /tmp/hdfs_load_generator.sh 500 5

# Restart the server
docker start <container_id_or_name>
```

### Simulate Network Partition

```bash
# Disconnect audit server from network
docker network disconnect rangernw <audit_server_container>

# Generate load
docker exec ranger-hadoop /tmp/hdfs_load_generator.sh 500 5

# Reconnect
docker network connect rangernw <audit_server_container>
```

### Simulate High Load / Stress Test

```bash
# Generate continuous heavy load for 10 minutes
(
  for i in {1..10}; do
    docker exec ranger-hadoop /tmp/hdfs_load_generator.sh 5000 50 &
    docker exec ranger-hive /tmp/hive_load_generator.sh 500 &
    sleep 60
  done
  wait
)

# Monitor during test
watch -n 2 'docker stats --no-stream | grep audit'
```

## Troubleshooting

### Issue: Scripts not executable
```bash
chmod +x *.sh
```

### Issue: Cannot connect to containers
```bash
# Check running containers
docker ps | grep -E "ranger-hadoop|ranger-hive|ranger-audit"

# Check network connectivity
docker exec ranger-hadoop ping -c 2 ranger-audit.rangernw
```

### Issue: Kafka consumer lag increasing
```bash
# Check lag
docker exec ranger-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group ranger_audit_solr_consumer_group \
  --describe

# Solution: Scale up audit servers
docker compose -f docker-compose.ranger-audit-haproxy.yml up -d --scale ranger-audit-svc=5
```

### Issue: HAProxy not distributing evenly
```bash
# Check HAProxy backend health
docker logs ranger-audit | grep -i "backend\|health"

# Check if all backends are UP
docker exec ranger-audit cat /usr/local/etc/haproxy/haproxy.cfg
```

## Performance Metrics

### Expected Throughput
- **HDFS Operations**: 100-500 ops/sec per thread
- **Audit Events**: 500-2500 events/sec (5 audits per HDFS op)
- **Hive Operations**: 10-50 ops/sec per operation type
- **Kafka Partition**: 10-20 MB/sec capacity

### Monitoring Commands

```bash
# CPU and Memory usage
docker stats --no-stream | grep audit

# Network I/O
docker stats --no-stream --format "table {{.Name}}\t{{.NetIO}}" | grep audit

# Kafka message count
docker exec ranger-kafka /opt/kafka/bin/kafka-run-class.sh \
  kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic ranger_audits --time -1
```

## Best Practices

1. **Start Small**: Begin with 2-3 audit servers and moderate load
2. **Monitor First**: Run `monitor_ha_load.sh` before generating heavy load
3. **Scale Gradually**: Increase servers one at a time and verify distribution
4. **Check Logs**: Review HAProxy and audit server logs for errors
5. **Clean Up**: Remove test data after load tests to avoid filling disk

## References

- [HA Load Simulation Guide](../../../../HA_LOAD_SIMULATION_GUIDE.md)
- [Audit Server Scaling Architecture](../../../../AUDIT_SERVER_SCALING_ARCHITECTURE.md)
- [HAProxy Configuration](../haproxy/haproxy-audit.cfg)
- [Docker Compose HA Setup](../../docker-compose.ranger-audit-haproxy.yml)

