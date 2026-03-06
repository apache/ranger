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
# Ranger Audit Server

This directory contains shell scripts to start and stop the Ranger Audit Server services locally (outside of Docker).

## Overview

The Ranger Audit Server consists of three microservices:

1. **ranger-audit-server-service** - Core audit server that receives audit events via REST API and produces them to Kafka
2. **ranger-audit-consumer-solr** - Consumer service that reads from Kafka and indexes audits to Solr
3. **ranger-audit-consumer-hdfs** - Consumer service that reads from Kafka and writes audits to HDFS/S3/Azure

Each service has its own `scripts` folder with start/stop scripts in its main directory.

## Prerequisites

Before running these scripts, ensure you have:

1. **Java 8 or higher** installed and `JAVA_HOME` set
2. **Built the project** using Maven:
   ```bash
   cd /path/to/ranger-audit-server
   mvn clean package -DskipTests
   ```
3. **Kafka running** (required for all services)
4. **Solr running** (required for Solr consumer)
5. **HDFS/Hadoop running** (required for HDFS consumer)

## Quick Start - All Services

### Start All Services

```bash
./scripts/start-all-services.sh
```

This script will start all three services in the correct order:
1. Audit Server (waits 10 seconds)
2. Solr Consumer (waits 5 seconds)
3. HDFS Consumer

### Stop All Services

```bash
./scripts/stop-all-services.sh
```

This script will stop all three services in reverse order.

## Individual Service Scripts

Each service can also be started/stopped individually:

### Audit Server Service

```bash
# Start
./ranger-audit-server-service/scripts/start-audit-server.sh

# Stop
./ranger-audit-server-service/scripts/stop-audit-server.sh
```

**Default Ports:** 7081 (HTTP), 7182 (HTTPS)
**Health Check:** http://localhost:7081/api/audit/health

### Solr Consumer

```bash
# Start
./ranger-audit-consumer-solr/scripts/start-consumer-solr.sh

# Stop
./ranger-audit-consumer-solr/scripts/stop-consumer-solr.sh
```

**Default Port:** 7091
**Health Check:** http://localhost:7091/api/health

### HDFS Consumer

```bash
# Start
./ranger-audit-consumer-hdfs/scripts/start-consumer-hdfs.sh

# Stop
./ranger-audit-consumer-hdfs/scripts/stop-consumer-hdfs.sh
```

**Default Port:** 7092
**Health Check:** http://localhost:7092/api/health

## Configuration

### Environment Variables

Each script supports the following environment variables:

#### Audit Server
- `AUDIT_SERVER_HOME_DIR` - Home directory (default: `target/`)
- `AUDIT_SERVER_CONF_DIR` - Configuration directory (default: `src/main/resources/conf/`)
- `AUDIT_SERVER_LOG_DIR` - Log directory (default: `logs/`)
- `AUDIT_SERVER_HEAP` - JVM heap settings (default: `-Xms512m -Xmx2g`)
- `AUDIT_SERVER_OPTS` - Additional JVM options
- `KERBEROS_ENABLED` - Enable Kerberos authentication (default: `false`)

#### Consumers (HDFS and Solr)
- `AUDIT_CONSUMER_HOME_DIR` - Home directory (default: `target/`)
- `AUDIT_CONSUMER_CONF_DIR` - Configuration directory (default: `src/main/resources/conf/`)
- `AUDIT_CONSUMER_LOG_DIR` - Log directory (default: `logs/`)
- `AUDIT_CONSUMER_HEAP` - JVM heap settings (default: `-Xms512m -Xmx2g`)
- `AUDIT_CONSUMER_OPTS` - Additional JVM options
- `KERBEROS_ENABLED` - Enable Kerberos authentication (default: `false`)

### Example with Custom Settings

```bash
# Set custom heap size and log directory
export AUDIT_SERVER_HEAP="-Xms1g -Xmx4g"
export AUDIT_SERVER_LOG_DIR="/var/log/ranger/range-audit-server"

./ranger-audit-server-service/scripts/start-audit-server.sh
```

## Log Files

Each service creates logs in its respective `logs/` directory (or custom location if set):

- **Audit Server:**
  - Application logs: `logs/ranger-audit-server.log`
  - Catalina output: `logs/catalina.out`
  - PID file: `logs/ranger-audit-server.pid`

- **Solr Consumer:**
  - Application logs: `logs/ranger-audit-consumer-solr.log`
  - Catalina output: `logs/catalina.out`
  - PID file: `logs/ranger-audit-consumer-solr.pid`

- **HDFS Consumer:**
  - Application logs: `logs/ranger-audit-consumer-hdfs.log`
  - Catalina output: `logs/catalina.out`
  - PID file: `logs/ranger-audit-consumer-hdfs.pid`

### Monitoring Logs

```bash
# Tail audit server logs
tail -f ranger-audit-server-service/logs/ranger-audit-server.log

# Tail Solr consumer logs
tail -f ranger-audit-consumer-solr/logs/ranger-audit-consumer-solr.log

# Tail HDFS consumer logs
tail -f ranger-audit-consumer-hdfs/logs/ranger-audit-consumer-hdfs.log
```

### Enabling Debug Logging

To enable debug logging for troubleshooting, modify the `logback.xml` configuration file in the service's `conf/` directory:

**For Audit Server:**
Edit `ranger-audit-server-service/src/main/resources/conf/logback.xml` (or `/opt/ranger-audit-server/conf/logback.xml` in Docker):

```xml
<!-- Change the root logger level from INFO to DEBUG -->
<root level="DEBUG">
    <appender-ref ref="LogToConsole" />
    <appender-ref ref="LogToRollingFile" />
</root>

<!-- Or enable debug for specific packages -->
<logger name="org.apache.ranger.audit" level="DEBUG" additivity="false">
    <appender-ref ref="LogToConsole" />
    <appender-ref ref="LogToRollingFile" />
</logger>
```

**For Consumers (HDFS/Solr):**
Similarly, edit the `logback.xml` in their respective `conf/` directories.

**Available log levels:** `TRACE`, `DEBUG`, `INFO`, `WARN`, `ERROR`

After modifying the logback configuration, restart the service for changes to take effect.

## Troubleshooting

### Service Won't Start

1. **Check if already running:**
   ```bash
   ps aux | grep ranger-audit
   ```

2. **Check for port conflicts:**
   ```bash
   lsof -i :7081  # Audit Server
   lsof -i :7091  # Solr Consumer
   lsof -i :7092  # HDFS Consumer
   ```

3. **Verify WAR file exists:**
   ```bash
   find ./target -name "*.war"
   ```

4. **Check logs for errors:**
   ```bash
   tail -100 logs/catalina.out
   ```

### Service Won't Stop

If a service doesn't stop gracefully, the script will force kill after 30 seconds. You can also manually kill:

```bash
# Find and kill the process
ps aux | grep "AuditServerApplication"
kill <PID>

# Or force kill
kill -9 <PID>

# Remove stale PID file
rm -f logs/ranger-audit-server.pid
```

### Java Not Found

If Java is not detected:

```bash
# Set JAVA_HOME
export JAVA_HOME=/path/to/java
export PATH=$JAVA_HOME/bin:$PATH

# Verify
java -version
```

### Kafka Connection Issues

Check Kafka bootstrap servers configuration in:
- `ranger-audit-server-service/src/main/resources/conf/ranger-audit-server-site.xml`
- `ranger-audit-consumer-solr/src/main/resources/conf/ranger-audit-consumer-solr-site.xml`
- `ranger-audit-consumer-hdfs/src/main/resources/conf/ranger-audit-consumer-hdfs-site.xml`

## Architecture

```
┌─────────────────────┐
│  Ranger Plugins     │
│  (HDFS, Hive, etc.) │
└──────────┬──────────┘
           │ REST API
           ▼
┌─────────────────────┐
│ Audit Server        │  Port 7081
│ (Producer)          │
└──────────┬──────────┘
           │ Kafka
           ▼
    ┌──────────────┐
    │    Kafka     │
    │   (Topic)    │
    └──────┬───────┘
           │
      ┌────┴────┬──────┬─────────┐
      │         │      │         │
      ▼         ▼      ▼         ▼
┌──────────┐ ┌──────────┐ ┌──────────┐     ┌──────────┐
│  Solr    │ │  HDFS    │ │  New     │ ... │   Nth    │
│ Consumer │ │ Consumer │ │ Consumer │     │ Consumer │
│ (7091)   │ │ (7092)   │ │ (709N)   │     │ (709N+1) │
└────┬─────┘ └────┬─────┘ └────┬─────┘     └────┬─────┘
     │            │            │                 │
     ▼            ▼            ▼                 ▼
┌─────────┐  ┌──────────┐ ┌──────────┐     ┌──────────┐
│  Solr   │  │   HDFS   │ │   New    │     │   Nth    │
│ (Index) │  │ (Storage)│ │(Dest)    │     │ (Dest)   │
└─────────┘  └──────────┘ └──────────┘     └──────────┘
```

## Adding a New Destination

To add a new audit destination (e.g., Elasticsearch, MongoDB, Cloud Storage, etc.), follow these steps:

### 1. Create a New Consumer Module

Create a new Maven module in the `ranger-audit-server` directory:

```bash
cd ranger-audit-server
mkdir ranger-audit-consumer-<destination>
cd ranger-audit-consumer-<destination>
```

Create a `pom.xml` based on the existing consumers (Solr or HDFS). Key dependencies:
- Spring Boot Starter
- Spring Kafka
- Your destination-specific client library (e.g., Elasticsearch client, MongoDB driver)

### 2. Implement the Consumer Application

Create the main Spring Boot application class:

```java
package org.apache.ranger.audit.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class YourDestinationConsumerApplication {
    public static void main(String[] args) {
        SpringApplication.run(YourDestinationConsumerApplication.class, args);
    }
}
```

### 3. Create the Kafka Consumer

Implement a Kafka consumer to read audit events:

```java
package org.apache.ranger.audit.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class YourDestinationConsumer {
    @KafkaListener(topics = "${ranger.audit.kafka.topic:ranger_audits}", groupId = "${ranger.audit.kafka.consumer.group:audit-consumer-your-destination}")
    public void consumeAudit(String auditEvent) {
        // Parse audit event
        // Transform if needed
        // Write to your destination
    }
}
```

### 4. Add Configuration Files

Create configuration files in `src/main/resources/conf/`:

**ranger-audit-consumer-<destination>-site.xml:**
```xml
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property>
        <name>ranger.audit.kafka.bootstrap.servers</name>
        <value>localhost:9092</value>
    </property>
    <property>
        <name>ranger.audit.kafka.topic</name>
        <value>ranger_audits</value>
    </property>
    <property>
        <name>ranger.audit.your-destination.url</name>
        <value>http://localhost:PORT</value>
    </property>
    <!-- Add destination-specific configurations -->
</configuration>
```

**application.yml:**
```yaml
server:
  port: 709X  # Choose next available port (e.g., 7093, 7094...)

spring:
  kafka:
    bootstrap-servers: ${ranger.audit.kafka.bootstrap.servers:localhost:9092}
    consumer:
      group-id: audit-consumer-your-destination
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer

# Add destination-specific Spring configurations
```

### 5. Create Start/Stop Scripts

Create a `scripts` directory with start/stop scripts:

**scripts/start-consumer-<destination>.sh:**
```bash
#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SERVICE_DIR="$(dirname "$SCRIPT_DIR")"

# Environment variables
AUDIT_CONSUMER_HOME_DIR="${AUDIT_CONSUMER_HOME_DIR:-$SERVICE_DIR/target}"
AUDIT_CONSUMER_CONF_DIR="${AUDIT_CONSUMER_CONF_DIR:-$SERVICE_DIR/src/main/resources/conf}"
AUDIT_CONSUMER_LOG_DIR="${AUDIT_CONSUMER_LOG_DIR:-$SERVICE_DIR/logs}"
AUDIT_CONSUMER_HEAP="${AUDIT_CONSUMER_HEAP:--Xms512m -Xmx2g}"
AUDIT_CONSUMER_OPTS="${AUDIT_CONSUMER_OPTS:-}"
KERBEROS_ENABLED="${KERBEROS_ENABLED:-false}"

# Find WAR file
WAR_FILE=$(find "$AUDIT_CONSUMER_HOME_DIR" -name "ranger-audit-consumer-<destination>*.war" | head -1)

if [ -z "$WAR_FILE" ]; then
    echo "Error: WAR file not found in $AUDIT_CONSUMER_HOME_DIR"
    exit 1
fi

# Start service
java $AUDIT_CONSUMER_HEAP $AUDIT_CONSUMER_OPTS \
    -Dlog.dir="$AUDIT_CONSUMER_LOG_DIR" \
    -Dconf.dir="$AUDIT_CONSUMER_CONF_DIR" \
    -jar "$WAR_FILE" > "$AUDIT_CONSUMER_LOG_DIR/catalina.out" 2>&1 &

echo $! > "$AUDIT_CONSUMER_LOG_DIR/ranger-audit-consumer-<destination>.pid"
echo "Started Ranger Audit Consumer (<destination>) with PID: $(cat $AUDIT_CONSUMER_LOG_DIR/ranger-audit-consumer-<destination>.pid)"
```

**scripts/stop-consumer-<destination>.sh:**
```bash
#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SERVICE_DIR="$(dirname "$SCRIPT_DIR")"
AUDIT_CONSUMER_LOG_DIR="${AUDIT_CONSUMER_LOG_DIR:-$SERVICE_DIR/logs}"
PID_FILE="$AUDIT_CONSUMER_LOG_DIR/ranger-audit-consumer-<destination>.pid"

if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    kill "$PID"
    echo "Stopped Ranger Audit Consumer (<destination>) with PID: $PID"
    rm -f "$PID_FILE"
else
    echo "PID file not found. Service may not be running."
fi
```

Make scripts executable:
```bash
chmod +x scripts/*.sh
```

### 6. Update Parent POM

Add the new module to the parent `ranger-audit-server/pom.xml`:

```xml
<modules>
    <module>ranger-audit-server-service</module>
    <module>ranger-audit-consumer-solr</module>
    <module>ranger-audit-consumer-hdfs</module>
    <module>ranger-audit-consumer-<destination></module>
</modules>
```

### 7. Update Start/Stop All Scripts

Add your consumer to `scripts/start-all-services.sh`:

```bash
# Start Your Destination Consumer
echo "Starting Ranger Audit Consumer (<destination>)..."
cd "$BASE_DIR/ranger-audit-consumer-<destination>"
./scripts/start-consumer-<destination>.sh
echo "Waiting 5 seconds for consumer to initialize..."
sleep 5
```

Add to `scripts/stop-all-services.sh`:

```bash
# Stop Your Destination Consumer
echo "Stopping Ranger Audit Consumer (<destination>)..."
cd "$BASE_DIR/ranger-audit-consumer-<destination>"
./scripts/stop-consumer-<destination>.sh
```

### 8. Build and Test

```bash
# Build the new consumer
cd ranger-audit-consumer-<destination>
mvn clean package -DskipTests

# Test individually
./scripts/start-consumer-<destination>.sh

# Check health (implement a health endpoint)
curl http://localhost:709X/api/health

# View logs
tail -f logs/ranger-audit-consumer-<destination>.log

# Stop when done
./scripts/stop-consumer-<destination>.sh
```

### 9. Add Documentation

Update this README to include:
- The new consumer in the "Overview" section
- Individual start/stop commands
- Default port and health check endpoint
- Configuration details specific to the destination
- Any prerequisite services required


## Development

### Building Individual Services

```bash
# Build specific service
cd ranger-audit-server-service
mvn clean package

cd ../ranger-audit-consumer-solr
mvn clean package

cd ../ranger-audit-consumer-hdfs
mvn clean package
```

### Running in Debug Mode

Add debug options to the `OPTS` environment variable:

```bash
export AUDIT_SERVER_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005"
./ranger-audit-server-service/scripts/start-audit-server.sh
```
Then attach your IDE debugger to port 5005.

