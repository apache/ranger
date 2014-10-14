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

# Replace the following:
#  %REPOSITORY_NAME%
#  %REPOSITORY_DESC%
#  %USERNAME%
#  %PASSWORD%
#  %JDBC_DRIVERCLASSNAME%
#  %JDBC_URL%
#  %COMMON_NAME_FOR_CERTIFICATE%
#
# Example:
#  INSERT INTO `x_asset` (asset_name, descr, act_status, asset_type, config, create_time, update_time, added_by_id, upd_by_id)
#   VALUES ('hivetest', 'hive test repo', 1, 3, '{\"username\":\"policymgr\",\"password\":\"policymgr\",\"jdbc.driverClassName\":\"org.apache.hive.jdbc.HiveDriver\",\"jdbc.url\":\"jdbc:hive2://sandbox.hortonworks.com:10000/default\",\"commonNameForCertificate\":\"\"}', now(), now(), 1, 1);
#

INSERT INTO `x_asset` (asset_name, descr, act_status, asset_type, config, create_time, update_time, added_by_id, upd_by_id)
 VALUES ('%REPOSITORY_NAME%', '%REPOSITORY_DESC%', 1, 3, '{\"username\":\"%USERNAME%\",\"password\":\"%PASSWORD%\",\"jdbc.driverClassName\":\"%JDBC_DRIVERCLASSNAME%\",\"jdbc.url\":\"%JDBC_URL%\",\"commonNameForCertificate\":\"%COMMON_NAME_FOR_CERTIFICATE%\"}', now(), now(), 1, 1);
SELECT @asset_id := id FROM x_asset WHERE asset_name='%REPOSITORY_NAME%' and act_status = 1;

# create default policy to allow access to public
INSERT INTO x_resource (policy_name, res_name, descr, res_type, asset_id, is_encrypt, is_recursive, res_dbs, res_tables, res_cols, res_status, table_type, col_type, create_time, update_time, added_by_id, upd_by_id) 
 VALUES ('default-hive', '/*/*/*', 'Default policy', 1, @asset_id, 2, 0, '*', '*', '*', 1, 0, 0, now(), now(), 1, 1);
SELECT @resource_id := id FROM x_resource WHERE policy_name='default-hive';

DELIMITER //
DROP PROCEDURE IF EXISTS CreateXAGroup;
CREATE PROCEDURE CreateXAGroup(in groupName varchar(1024))
BEGIN
  DECLARE groupId bigint(20);

  SELECT g.id INTO groupId FROM x_group g WHERE g.group_name = groupName;

  IF groupId IS NULL THEN
	SELECT CONCAT('Creating group ', groupName);
    INSERT INTO x_group (group_name, descr, status, group_type, create_time, update_time, added_by_id, upd_by_id) VALUES (groupName, groupName, 0, 1, now(), now(), 1, 1);
  ELSE
    SELECT CONCAT('Group ', groupName, ' already exists');
  END IF;
END //
DELIMITER ;
CALL CreateXAGroup('public');
DROP PROCEDURE IF EXISTS CreateXAGroup;

SELECT @group_public := id FROM x_group WHERE group_name='public';

SELECT @perm_create := 4;
SELECT @perm_select := 10;
SELECT @perm_update := 11;
SELECT @perm_drop   := 12;
SELECT @perm_alter  := 13;
SELECT @perm_index  := 14;
SELECT @perm_lock   := 15;
SELECT @perm_all    := 16;
SELECT @perm_admin  := 6;


INSERT INTO x_perm_map (res_id, group_id, perm_for, perm_type, perm_group, is_recursive, is_wild_card, grant_revoke, create_time, update_time, added_by_id, upd_by_id) VALUES (@resource_id, @group_public, 2, @perm_create, now(), 0, 1, 1, now(), now(), 1, 1);

INSERT INTO x_perm_map (res_id, group_id, perm_for, perm_type, perm_group, is_recursive, is_wild_card, grant_revoke, create_time, update_time, added_by_id, upd_by_id) VALUES (@resource_id, @group_public, 2, @perm_select, now(), 0, 1, 1, now(), now(), 1, 1);

INSERT INTO x_perm_map (res_id, group_id, perm_for, perm_type, perm_group, is_recursive, is_wild_card, grant_revoke, create_time, update_time, added_by_id, upd_by_id) VALUES (@resource_id, @group_public, 2, @perm_update, now(), 0, 1, 1, now(), now(), 1, 1);

INSERT INTO x_perm_map (res_id, group_id, perm_for, perm_type, perm_group, is_recursive, is_wild_card, grant_revoke, create_time, update_time, added_by_id, upd_by_id) VALUES (@resource_id, @group_public, 2, @perm_drop, now(), 0, 1, 1, now(), now(), 1, 1);

INSERT INTO x_perm_map (res_id, group_id, perm_for, perm_type, perm_group, is_recursive, is_wild_card, grant_revoke, create_time, update_time, added_by_id, upd_by_id) VALUES (@resource_id, @group_public, 2, @perm_alter, now(), 0, 1, 1, now(), now(), 1, 1);

INSERT INTO x_perm_map (res_id, group_id, perm_for, perm_type, perm_group, is_recursive, is_wild_card, grant_revoke, create_time, update_time, added_by_id, upd_by_id) VALUES (@resource_id, @group_public, 2, @perm_index, now(), 0, 1, 1, now(), now(), 1, 1);

INSERT INTO x_perm_map (res_id, group_id, perm_for, perm_type, perm_group, is_recursive, is_wild_card, grant_revoke, create_time, update_time, added_by_id, upd_by_id) VALUES (@resource_id, @group_public, 2, @perm_lock, now(), 0, 1, 1, now(), now(), 1, 1);

INSERT INTO x_perm_map (res_id, group_id, perm_for, perm_type, perm_group, is_recursive, is_wild_card, grant_revoke, create_time, update_time, added_by_id, upd_by_id) VALUES (@resource_id, @group_public, 2, @perm_all, now(), 0, 1, 1, now(), now(), 1, 1);

INSERT INTO x_perm_map (res_id, group_id, perm_for, perm_type, perm_group, is_recursive, is_wild_card, grant_revoke, create_time, update_time, added_by_id, upd_by_id) VALUES (@resource_id, @group_public, 2, @perm_admin, now(), 0, 1, 1, now(), now(), 1, 1);

# Enable auditing
INSERT INTO x_audit_map (res_id, audit_type, create_time, update_time, added_by_id, upd_by_id) VALUES (@resource_id, 1, now(), now(), 1, 1);
