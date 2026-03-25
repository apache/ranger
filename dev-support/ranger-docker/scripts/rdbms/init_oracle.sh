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


set -e

export ORACLE_SID=FREE

sqlplus / AS SYSDBA  <<EOSQL
    ALTER SESSION SET container=FREEPDB1;

    CREATE TABLESPACE ranger DATAFILE '/tmp/ranger.dbf' SIZE 50m ONLINE;
    CREATE USER rangeradmin IDENTIFIED BY "rangerR0cks!" DEFAULT TABLESPACE ranger QUOTA 50m ON ranger ACCOUNT UNLOCK;
    GRANT SELECT_CATALOG_ROLE TO rangeradmin;
    GRANT CONNECT, RESOURCE TO rangeradmin;
    GRANT CREATE SESSION, CREATE PROCEDURE, CREATE TABLE, CREATE VIEW, CREATE SEQUENCE, CREATE PUBLIC SYNONYM, CREATE ANY SYNONYM, CREATE TRIGGER, UNLIMITED TABLESPACE TO rangeradmin;

    CREATE TABLESPACE rangerkms DATAFILE '/tmp/rangerkms.dbf' SIZE 10m ONLINE;
    CREATE USER rangerkms IDENTIFIED BY "rangerR0cks!" DEFAULT TABLESPACE rangerkms QUOTA 25m ON rangerkms ACCOUNT UNLOCK;
    GRANT SELECT_CATALOG_ROLE TO rangerkms;
    GRANT CONNECT, RESOURCE TO rangerkms;
    GRANT CREATE SESSION, CREATE PROCEDURE, CREATE TABLE, CREATE VIEW, CREATE SEQUENCE, CREATE PUBLIC SYNONYM, CREATE ANY SYNONYM, CREATE TRIGGER, UNLIMITED TABLESPACE TO rangerkms;


    CREATE TABLESPACE hive DATAFILE '/tmp/hive.dbf' SIZE 25m ONLINE;
    CREATE USER hive IDENTIFIED BY "rangerR0cks!" DEFAULT TABLESPACE hive QUOTA 25m ON hive ACCOUNT UNLOCK;
    GRANT SELECT_CATALOG_ROLE TO hive;
    GRANT CONNECT,  RESOURCE TO hive;
    GRANT CREATE SESSION, CREATE PROCEDURE, CREATE TABLE, CREATE VIEW, CREATE SEQUENCE, CREATE PUBLIC SYNONYM, CREATE ANY SYNONYM, CREATE TRIGGER, UNLIMITED TABLESPACE TO hive;
EOSQL
