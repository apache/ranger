-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

USE ranger;
SET @current_date_time  =  NOW();
SET @ranger_admin_username = 'ranger_username';
-- ranger admin creates services
SET @ranger_user_id = (SELECT xpu.id FROM x_portal_user xpu WHERE xpu.login_id = @ranger_admin_username);
-- Create TAG services first for hdfs and hive
SET @tag_service_type = (SELECT xsd.id FROM x_service_def xsd WHERE xsd.name = 'tag');
SET @service_guid = uuid();
-- Insert HDFS Tag
SET @hdfs_tag = 'hdfs';
INSERT INTO x_service (guid, create_time, update_time, added_by_id, upd_by_id, version, type, name, description, is_enabled, tag_version) VALUES (@service_guid, @current_date_time, @current_date_time, @ranger_user_id, @ranger_user_id, 1, @tag_service_type, @hdfs_tag, 'HDFS Tag', 1, 1);
-- Insert Hive Tag
SET @hive_tag = 'hive';
SET @service_guid = uuid();
INSERT INTO x_service (guid, create_time, update_time, added_by_id, upd_by_id, version, type, name, description, is_enabled, tag_version) VALUES (@service_guid, @current_date_time, @current_date_time, @ranger_user_id, @ranger_user_id, 1, @tag_service_type, @hive_tag, 'Hive Tag', 1, 1);
-- Create HDFS service
SET @hdfs_service_type = (SELECT xsd.id FROM x_service_def xsd WHERE xsd.name = @hdfs_tag);
SET @hdfs_service_name = 'hdfs_service';
-- Retrieve Tag Service ID for hdfs
SET @hdfs_tag_service = (SELECT xs.id FROM x_service xs WHERE xs.type = @tag_service_type AND xs.name = @hdfs_tag);
SET @hdfs_tag_service_version = (SELECT xs.version FROM x_service xs WHERE xs.id = @hdfs_tag_service);
SET @service_guid = uuid();
INSERT INTO x_service (guid, create_time, update_time, added_by_id, upd_by_id, version, type, name, description, is_enabled, tag_service, tag_version) VALUES (@service_guid, @current_date_time, @current_date_time, @ranger_user_id, @ranger_user_id, 1, @hdfs_service_type, @hdfs_service_name, 'Hadoop Environment', 1, @hdfs_tag_service, @hdfs_tag_service_version);
SET @hdfs_service_id = (SELECT LAST_INSERT_ID());
-- Insert configurations for HDFS
INSERT INTO x_service_config_map (create_time, update_time, added_by_id, upd_by_id, service, config_key, config_value) VALUES (@current_date_time, @current_date_time, @ranger_user_id, @ranger_user_id, @hdfs_service_id, 'hadoop.security.authentication', 'kerberos');
INSERT INTO x_service_config_map (create_time, update_time, added_by_id, upd_by_id, service, config_key, config_value) VALUES (@current_date_time, @current_date_time, @ranger_user_id, @ranger_user_id, @hdfs_service_id, 'hadoop.rpc.protection', 'authentication');
INSERT INTO x_service_config_map (create_time, update_time, added_by_id, upd_by_id, service, config_key, config_value) VALUES (@current_date_time, @current_date_time, @ranger_user_id, @ranger_user_id, @hdfs_service_id, 'fs.default.name', 'hdfs://localhost:port');
INSERT INTO x_service_config_map (create_time, update_time, added_by_id, upd_by_id, service, config_key, config_value) VALUES (@current_date_time, @current_date_time, @ranger_user_id, @ranger_user_id, @hdfs_service_id, 'hadoop.security.authorization', 'true');
INSERT INTO x_service_config_map (create_time, update_time, added_by_id, upd_by_id, service, config_key, config_value) VALUES (@current_date_time, @current_date_time, @ranger_user_id, @ranger_user_id, @hdfs_service_id, 'username', @ranger_admin_username);
-- Enable deny and exclude policies by default for HDFS
UPDATE x_service_def SET def_options = "{\"enableDenyAndExceptionsInPolicies\":\"true\"}" WHERE id = @hdfs_service_type;
-- Create Hive service
SET @hive_service_type = (SELECT xsd.id FROM x_service_def xsd WHERE xsd.name = @hive_tag);
SET @hive_service_name = 'hive_service';
-- Retrieve Tag Service ID for hive
SET @hive_tag_service = (SELECT xs.id FROM x_service xs WHERE xs.type = @tag_service_type AND xs.name = @hive_tag);
SET @hive_tag_service_version = (SELECT xs.version FROM x_service xs WHERE xs.id = @hive_tag_service);
SET @service_guid = uuid();
INSERT INTO x_service (guid, create_time, update_time, added_by_id, upd_by_id, version, type, name, description, is_enabled, tag_service, tag_version) VALUES (@service_guid, @current_date_time, @current_date_time, @ranger_user_id, @ranger_user_id, 1, @hive_service_type, @hive_service_name, 'Hive Environment', 1, @hive_tag_service, @hive_tag_service_version);
SET @hive_service_id = (SELECT LAST_INSERT_ID());
-- Insert configurations for HIVE
INSERT INTO x_service_config_map (create_time, update_time, added_by_id, upd_by_id, service, config_key, config_value) VALUES (@current_date_time, @current_date_time, @ranger_user_id, @ranger_user_id, @hive_service_id, 'jdbc.driverClassName', 'org.apache.hive.jdbc.HiveDriver');
INSERT INTO x_service_config_map (create_time, update_time, added_by_id, upd_by_id, service, config_key, config_value) VALUES (@current_date_time, @current_date_time, @ranger_user_id, @ranger_user_id, @hive_service_id, 'jdbc.url', '');
INSERT INTO x_service_config_map (create_time, update_time, added_by_id, upd_by_id, service, config_key, config_value) VALUES (@current_date_time, @current_date_time, @ranger_user_id, @ranger_user_id, @hive_service_id, 'username', @ranger_admin_username);
-- Enable deny and exclude policies by default for HIVE
UPDATE x_service_def SET def_options = "{\"enableDenyAndExceptionsInPolicies\":\"true\"}" WHERE id = @hive_service_type;