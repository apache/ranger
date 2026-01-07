#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Hive Load Generator for Ranger Audit Testing
# This script generates Hive SQL operations to create audit events

NUM_OPERATIONS=${1:-100}
DB_NAME="audit_load_test_$(date +%s)"
TABLE_NAME="test_table"

echo "============================================"
echo "Hive Load Generator for Ranger Audit"
echo "============================================"
echo "Operations: $NUM_OPERATIONS"
echo "Database: $DB_NAME"
echo "Table: $TABLE_NAME"
echo "Estimated Audit Events: ~$((NUM_OPERATIONS * 3))"
echo "============================================"

start_time=$(date +%s)
echo ""
echo "Starting Hive load generation at $(date)"
echo ""

# Create SQL script
SQL_SCRIPT="/tmp/hive_load_${DB_NAME}.sql"

cat > $SQL_SCRIPT << EOSQL
-- Create test database
CREATE DATABASE IF NOT EXISTS ${DB_NAME};
USE ${DB_NAME};

-- Create test table
CREATE TABLE IF NOT EXISTS ${TABLE_NAME} (
  id INT,
  name STRING,
  value DOUBLE,
  timestamp_col TIMESTAMP,
  category STRING
) 
PARTITIONED BY (year INT, month INT)
STORED AS ORC;

-- Insert operations (each generates audit event)
EOSQL

# Generate insert statements
echo "Generating $NUM_OPERATIONS insert statements..."
for i in $(seq 1 $NUM_OPERATIONS); do
  year=$((2020 + RANDOM % 5))
  month=$((1 + RANDOM % 12))
  value=$(echo "scale=2; $i * 1.5 + ($RANDOM % 100)" | bc)
  cat >> $SQL_SCRIPT << EOSQL
INSERT INTO ${TABLE_NAME} PARTITION(year=$year, month=$month) 
  VALUES ($i, 'name_$i', $value, current_timestamp(), 'category_$((i % 10))');
EOSQL
done

# Generate select operations
cat >> $SQL_SCRIPT << EOSQL

-- Select operations (each generates audit event)
SELECT COUNT(*) as total_count FROM ${TABLE_NAME};
SELECT * FROM ${TABLE_NAME} LIMIT 10;
SELECT year, month, COUNT(*) FROM ${TABLE_NAME} GROUP BY year, month;
SELECT category, AVG(value) as avg_value FROM ${TABLE_NAME} GROUP BY category;
SELECT * FROM ${TABLE_NAME} WHERE id > $((NUM_OPERATIONS / 2)) LIMIT 5;
SELECT MAX(value), MIN(value), AVG(value) FROM ${TABLE_NAME};

-- Show operations
SHOW TABLES IN ${DB_NAME};
SHOW PARTITIONS ${TABLE_NAME};
DESCRIBE ${TABLE_NAME};
DESCRIBE FORMATTED ${TABLE_NAME};

-- Alter operations
ALTER TABLE ${TABLE_NAME} ADD COLUMNS (new_field STRING);
ALTER TABLE ${TABLE_NAME} SET TBLPROPERTIES ('comment' = 'Audit load test table');

-- Create view
CREATE VIEW IF NOT EXISTS ${TABLE_NAME}_view AS 
  SELECT id, name, value FROM ${TABLE_NAME} WHERE value > 100;

-- Query view
SELECT * FROM ${TABLE_NAME}_view LIMIT 5;

-- Drop view
DROP VIEW IF EXISTS ${TABLE_NAME}_view;

-- Cleanup
DROP TABLE IF EXISTS ${TABLE_NAME};
DROP DATABASE IF EXISTS ${DB_NAME};

EOSQL

echo "SQL script generated: $SQL_SCRIPT"
echo "Executing Hive operations via beeline..."
echo ""

# Execute the SQL script
beeline -u "jdbc:hive2://localhost:10000" -f $SQL_SCRIPT --silent=false

exit_code=$?

end_time=$(date +%s)
duration=$((end_time - start_time))

echo ""
echo "============================================"
echo "Hive Load Generation Complete"
echo "============================================"
echo "Duration: $duration seconds"
echo "Operations: $NUM_OPERATIONS inserts + queries"
echo "Exit Code: $exit_code"
echo "============================================"
echo ""

if [ $exit_code -eq 0 ]; then
  echo "✓ Success: All Hive operations completed"
  echo ""
  echo "To check audit events in Kafka:"
  echo "  kafka-console-consumer.sh --bootstrap-server localhost:9092 \\"
  echo "    --topic ranger_audits --max-messages 10"
else
  echo "✗ Error: Some Hive operations failed (exit code: $exit_code)"
  echo "SQL script preserved at: $SQL_SCRIPT"
fi

echo ""

