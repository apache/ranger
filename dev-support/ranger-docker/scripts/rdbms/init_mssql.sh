#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License,  Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

/opt/mssql/bin/sqlservr &

# Wait for SQL Server to be ready
echo "Waiting for SQL Server to start..."
RETRIES=30  # Number of retries
SLEEP_INTERVAL=5  # Seconds to wait between retries
for i in $(seq 1 $RETRIES); do
    # Try to connect to SQL Server
    /opt/mssql-tools18/bin/sqlcmd -S localhost -U SA -P "rangerR0cks!" -Q "SELECT 1" -C > /dev/null 2>&1
    if [ $? -eq 0 ]; then
        echo "SQL Server is ready!"
        break
    else
        echo "SQL Server is not ready yet. Waiting..."
        sleep $SLEEP_INTERVAL
    fi
done

if [ $i -eq $RETRIES ]; then
    echo "SQL Server did not become ready in time. Exiting."
    exit 1
fi

/opt/mssql-tools18/bin/sqlcmd -S localhost -U SA -P 'rangerR0cks!' -Q "

-- Set the database context
USE master;

-- Create databases
CREATE DATABASE ranger;
CREATE DATABASE rangerkms;
CREATE DATABASE hive;
GO

-- Create users and assign permissions
USE ranger;
CREATE LOGIN rangeradmin WITH PASSWORD = 'rangerR0cks!';
CREATE USER rangeradmin FOR LOGIN rangeradmin;
ALTER ROLE db_owner ADD MEMBER rangeradmin; -- Grant equivalent high-level permissions
GO

USE rangerkms;
CREATE LOGIN rangerkms WITH PASSWORD = 'rangerR0cks!';
CREATE USER rangerkms FOR LOGIN rangerkms;
ALTER ROLE db_owner ADD MEMBER rangerkms; -- Grant equivalent high-level permissions
GO

USE hive;
CREATE LOGIN hive WITH PASSWORD = 'rangerR0cks!';
CREATE USER hive FOR LOGIN hive;
ALTER ROLE db_owner ADD MEMBER hive; -- Grant equivalent high-level permissions
GO
" -C

# Bring SQL Server to the foreground
wait -n
exec /opt/mssql/bin/sqlservr
