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


DELIMITER $$
DROP PROCEDURE if exists getXportalUIdByLoginId$$
CREATE PROCEDURE `getXportalUIdByLoginId`(IN input_val VARCHAR(100), OUT myid BIGINT)
BEGIN
SET myid = 0;
SELECT x_portal_user.id into myid FROM x_portal_user WHERE x_portal_user.login_id = input_val;
END $$

DELIMITER ;

DELIMITER $$
DROP PROCEDURE if exists getModulesIdByName$$
CREATE PROCEDURE `getModulesIdByName`(IN input_val VARCHAR(100), OUT myid BIGINT)
BEGIN
SET myid = 0;
SELECT x_modules_master.id into myid FROM x_modules_master WHERE x_modules_master.module = input_val;
END $$

DELIMITER ;


DELIMITER $$
DROP PROCEDURE if exists insertRangerPrerequisiteGDSEntries $$
CREATE PROCEDURE `insertRangerPrerequisiteGDSEntries`()
BEGIN
DECLARE adminID bigint;
DECLARE moduleIdGovernedDataSharing bigint;
DECLARE rangerusersyncID bigint;
DECLARE rangertagsyncID bigint;

call getXportalUIdByLoginId('admin', adminID);
call getXportalUIdByLoginId('rangerusersync', rangerusersyncID);
call getXportalUIdByLoginId('rangertagsync', rangertagsyncID);

if not exists (select * from x_modules_master where module='Governed Data Sharing') then
  INSERT INTO `x_modules_master` (`create_time`,`update_time`,`added_by_id`,`upd_by_id`,`module`,`url`) VALUES (UTC_TIMESTAMP(),UTC_TIMESTAMP(),adminID,adminID,'Governed Data Sharing','');
end if;
call getModulesIdByName('Governed Data Sharing', moduleIdGovernedDataSharing);
if not exists (select * from x_user_module_perm where user_id=adminID and module_id=moduleIdGovernedDataSharing) then
  INSERT INTO x_user_module_perm (user_id,module_id,create_time,update_time,added_by_id,upd_by_id,is_allowed) VALUES (adminID,moduleIdGovernedDataSharing,UTC_TIMESTAMP(),UTC_TIMESTAMP(),adminID,adminID,1);
end if;
if not exists (select * from x_user_module_perm where user_id=rangerusersyncID and module_id=moduleIdGovernedDataSharing) then
  INSERT INTO x_user_module_perm (user_id,module_id,create_time,update_time,added_by_id,upd_by_id,is_allowed) VALUES (rangerusersyncID,moduleIdGovernedDataSharing,UTC_TIMESTAMP(),UTC_TIMESTAMP(),adminID,adminID,1);
end if;
if not exists (select * from x_user_module_perm where user_id=rangertagsyncID and module_id=moduleIdGovernedDataSharing) then
  INSERT INTO x_user_module_perm (user_id,module_id,create_time,update_time,added_by_id,upd_by_id,is_allowed) VALUES (rangertagsyncID,moduleIdGovernedDataSharing,UTC_TIMESTAMP(),UTC_TIMESTAMP(),adminID,adminID,1);
end if;

END $$
DELIMITER ;
call insertRangerPrerequisiteGDSEntries();
