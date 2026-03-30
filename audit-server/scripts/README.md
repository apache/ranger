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

1. **ranger-audit-ingestor** - Core audit server that receives audit events via REST API and produces them to Kafka
2. **ranger-audit-dispatcher-solr** - Dispatcher service that reads from Kafka and indexes audits to Solr
3. **ranger-audit-dispatcher-hdfs** - Dispatcher service that reads from Kafka and writes audits to HDFS/S3/Azure

The codebase is organized into two main directories:
- `audit-ingestor/`
- `audit-dispatcher/` (contains a unified dispatcher application that dynamically loads specific dispatcher types)

## Prerequisites

Before running these scripts, ensure you have:

1. **Java 8 or higher** installed and `JAVA_HOME` set
2. **Built the project** using Maven:
   ```bash
   cd /path/to/ranger-audit-server
   mvn clean package -DskipTests
   ```
3. **Kafka running** (required for all services)
4. **Solr running** (required for Solr dispatcher)
5. **HDFS/Hadoop running** (required for HDFS dispatcher)

## Quick Start - All Services

### Start All Services

```bash
./scripts/start-all-services.sh
```

This script will start all three services in the correct order:
1. Audit Server (waits 10 seconds)
2. Solr Dispatcher (waits 5 seconds)
3. HDFS Dispatcher

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
./audit-ingestor/scripts/start-audit-ingestor.sh

# Stop
./audit-ingestor/scripts/stop-audit-ingestor.sh
```

**Default Ports:** 7081 (HTTP), 7182 (HTTPS)
**Health Check:** http://localhost:7081/api/audit/health

### Solr Dispatcher

```bash
# Start
./audit-dispatcher/scripts/start-audit-dispatcher.sh solr

# Stop
./audit-dispatcher/scripts/stop-audit-dispatcher.sh solr
```

**Default Port:** 7091
**Health Check:** http://localhost:7091/api/health/ping

### HDFS Dispatcher

```bash
# Start
./audit-dispatcher/scripts/start-audit-dispatcher.sh hdfs

# Stop
./audit-dispatcher/scripts/stop-audit-dispatcher.sh hdfs
```

**Default Port:** 7092
**Health Check:** http://localhost:7092/api/health/ping

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

#### Dispatchers (HDFS and Solr)
- `AUDIT_DISPATCHER_HOME_DIR` - Home directory (default: `target/`)
- `AUDIT_DISPATCHER_CONF_DIR` - Configuration directory (default: `src/main/resources/conf/`)
- `AUDIT_DISPATCHER_LOG_DIR` - Log directory (default: `logs/`)
- `AUDIT_DISPATCHER_HEAP` - JVM heap settings (default: `-Xms512m -Xmx2g`)
- `AUDIT_DISPATCHER_OPTS` - Additional JVM options
- `KERBEROS_ENABLED` - Enable Kerberos authentication (default: `false`)

### Example with Custom Settings

```bash
# Set custom heap size and log directory
export AUDIT_SERVER_HEAP="-Xms1g -Xmx4g"
export AUDIT_SERVER_LOG_DIR="/var/log/ranger/range-audit-server"

./audit-ingestor/scripts/start-audit-ingestor.sh
```

## Log Files

Each service creates logs in its respective `logs/` directory (or custom location if set):

- **Audit Server:**
  - Application logs: `logs/ranger-audit-ingestor.log`
  - Catalina output: `logs/catalina.out`
  - PID file: `logs/ranger-audit-ingestor.pid`

- **Solr Dispatcher:**
  - Application logs: `logs/ranger-audit-dispatcher.log`
  - Catalina output: `logs/catalina.out`
  - PID file: `logs/ranger-audit-dispatcher-solr.pid`

- **HDFS Dispatcher:**
  - Application logs: `logs/ranger-audit-dispatcher.log`
  - Catalina output: `logs/catalina.out`
  - PID file: `logs/ranger-audit-dispatcher-hdfs.pid`

### Monitoring Logs

```bash
# Tail audit server logs
tail -f audit-ingestor/logs/ranger-audit-ingestor.log

# Tail Solr dispatcher logs
tail -f audit-dispatcher/logs/ranger-audit-dispatcher.log

# Tail HDFS dispatcher logs
tail -f audit-dispatcher/logs/ranger-audit-dispatcher.log
```

### Enabling Debug Logging

To enable debug logging for troubleshooting, modify the `logback.xml` configuration file in the service's `conf/` directory:

**For Audit Server:**
Edit `audit-ingestor/src/main/resources/conf/logback.xml` (or `/opt/ranger/audit-ingestor/conf/logback.xml` in Docker):

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

**For Dispatchers (HDFS/Solr):**
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
   lsof -i :7091  # Solr Dispatcher
   lsof -i :7092  # HDFS Dispatcher
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
rm -f logs/ranger-audit-ingestor.pid
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
- `audit-ingestor/src/main/resources/conf/ranger-audit-ingestor-site.xml`
- `audit-dispatcher/dispatcher-solr/src/main/resources/conf/ranger-audit-dispatcher-solr-site.xml`
- `audit-dispatcher/dispatcher-hdfs/src/main/resources/conf/ranger-audit-dispatcher-hdfs-site.xml`

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
│ Dispatcher │ │ Dispatcher │ │ Dispatcher │     │ Dispatcher │
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

### 1. Create a New Dispatcher Module

Create a new Maven module in the `audit-server/audit-dispatcher` directory:

```bash
cd audit-server/audit-dispatcher
mkdir dispatcher-<destination>
cd dispatcher-<destination>
```

Create a `pom.xml` based on the existing dispatchers (Solr or HDFS). Key dependencies:
- `ranger-audit-dispatcher-common` (provided scope)
- Your destination-specific client library (e.g., Elasticsearch client, MongoDB driver)

### 2. Implement the Dispatcher Manager and Kafka Dispatcher

Create a `DispatcherManager` class that implements the singleton pattern and a `KafkaDispatcher` class that extends `AuditDispatcherBase`:

```java
package org.apache.ranger.audit.dispatcher;

public class YourDestinationDispatcherManager {
    // Implement singleton and initialization logic
    // See SolrDispatcherManager for reference
}
```

```java
package org.apache.ranger.audit.dispatcher.kafka;

public class AuditYourDestinationDispatcher extends AuditDispatcherBase {
    // Implement consume logic
    // See AuditSolrDispatcher for reference
}
```

### 3. Add Configuration Files

Create configuration files in `src/main/resources/conf/`:

**ranger-audit-dispatcher-<destination>-site.xml:**
```xml
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property>
        <name>ranger.audit.dispatcher.type</name>
        <value>your-destination</value>
    </property>
    <!-- Add destination-specific configurations -->
</configuration>
```

### 4. Update Parent POM and Assembly

Add the new module to the parent `audit-server/audit-dispatcher/pom.xml`:

```xml
<modules>
    <module>dispatcher-common</module>
    <module>dispatcher-solr</module>
    <module>dispatcher-hdfs</module>
    <module>dispatcher-<destination></module>
    <module>dispatcher-app</module>
</modules>
```

Update `distro/src/main/assembly/audit-dispatcher.xml` to include the new module's JARs in `lib/dispatchers/<destination>`.

### 5. Build and Test

```bash
# Build the project
mvn clean package -DskipTests

# Test individually
./audit-dispatcher/scripts/start-audit-dispatcher.sh <destination>

# Check health
curl http://localhost:709X/api/health/ping

# View logs
tail -f audit-dispatcher/logs/ranger-audit-dispatcher.log

# Stop when done
./audit-dispatcher/scripts/stop-audit-dispatcher.sh <destination>
```

### 6. Add Documentation

Update this README to include:
- The new dispatcher in the "Overview" section
- Individual start/stop commands
- Default port and health check endpoint
- Configuration details specific to the destination
- Any prerequisite services required


## Development

### Building Individual Services

```bash
# Build specific service
cd audit-ingestor
mvn clean package

cd ../audit-dispatcher
mvn clean package
```

### Running in Debug Mode

Add debug options to the `OPTS` environment variable:

```bash
export AUDIT_SERVER_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005"
./audit-ingestor/scripts/start-audit-ingestor.sh
```
Then attach your IDE debugger to port 5005.

