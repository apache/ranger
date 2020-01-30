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

drop procedure if exists drop_table_column;
delimiter ;;
create procedure drop_table_column(IN tableName varchar(64), IN columnName varchar(64)) begin
  if exists (select * from information_schema.columns where table_schema=database() and table_name = tableName and column_name = columnName) then
    SET @query = CONCAT('ALTER TABLE `', tableName,'` DROP COLUMN `', columnName,'`');
    PREPARE stmt FROM @query;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
  end if;
end;;
delimiter ;

drop procedure if exists drop_table_foreign_key;
delimiter ;;
create procedure drop_table_foreign_key(IN tableName varchar(64), IN foreignKeyName varchar(64)) begin
  if exists (select * from information_schema.table_constraints where table_schema=database() and table_name = tableName and constraint_name = foreignKeyName and constraint_type = 'FOREIGN KEY') then
    SET @query = CONCAT('ALTER TABLE `', tableName,'` DROP FOREIGN KEY `', foreignKeyName,'`');
    PREPARE stmt FROM @query;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
  end if;
end;;
delimiter ;

call drop_table_column('x_policy_ref_resource', 'create_time');
call drop_table_column('x_policy_ref_resource', 'update_time');
call drop_table_foreign_key('x_policy_ref_resource', 'x_policy_ref_res_FK_added_by_id');
call drop_table_column('x_policy_ref_resource', 'added_by_id');
call drop_table_foreign_key('x_policy_ref_resource', 'x_policy_ref_res_FK_upd_by_id');
call drop_table_column('x_policy_ref_resource', 'upd_by_id');

call drop_table_column('x_policy_ref_role', 'create_time');
call drop_table_column('x_policy_ref_role', 'update_time');
call drop_table_foreign_key('x_policy_ref_role', 'x_policy_ref_role_FK_added_by_id');
call drop_table_column('x_policy_ref_role', 'added_by_id');
call drop_table_foreign_key('x_policy_ref_role', 'x_policy_ref_role_FK_upd_by_id');
call drop_table_column('x_policy_ref_role', 'upd_by_id');

call drop_table_column('x_policy_ref_group', 'create_time');
call drop_table_column('x_policy_ref_group', 'update_time');
call drop_table_foreign_key('x_policy_ref_group', 'x_policy_ref_group_FK_added_by_id');
call drop_table_column('x_policy_ref_group', 'added_by_id');
call drop_table_foreign_key('x_policy_ref_group', 'x_policy_ref_group_FK_upd_by_id');
call drop_table_column('x_policy_ref_group', 'upd_by_id');

call drop_table_column('x_policy_ref_user', 'create_time');
call drop_table_column('x_policy_ref_user', 'update_time');
call drop_table_foreign_key('x_policy_ref_user', 'x_policy_ref_user_FK_added_by_id');
call drop_table_column('x_policy_ref_user', 'added_by_id');
call drop_table_foreign_key('x_policy_ref_user', 'x_policy_ref_user_FK_upd_by_id');
call drop_table_column('x_policy_ref_user', 'upd_by_id');

call drop_table_column('x_policy_ref_access_type', 'create_time');
call drop_table_column('x_policy_ref_access_type', 'update_time');
call drop_table_foreign_key('x_policy_ref_access_type', 'x_policy_ref_access_FK_added_by_id');
call drop_table_column('x_policy_ref_access_type', 'added_by_id');
call drop_table_foreign_key('x_policy_ref_access_type', 'x_policy_ref_access_FK_upd_by_id');
call drop_table_column('x_policy_ref_access_type', 'upd_by_id');

call drop_table_column('x_policy_ref_condition', 'create_time');
call drop_table_column('x_policy_ref_condition', 'update_time');
call drop_table_foreign_key('x_policy_ref_condition', 'x_policy_ref_condition_FK_added_by_id');
call drop_table_column('x_policy_ref_condition', 'added_by_id');
call drop_table_foreign_key('x_policy_ref_condition', 'x_policy_ref_condition_FK_upd_by_id');
call drop_table_column('x_policy_ref_condition', 'upd_by_id');

call drop_table_column('x_policy_ref_datamask_type', 'create_time');
call drop_table_column('x_policy_ref_datamask_type', 'update_time');
call drop_table_foreign_key('x_policy_ref_datamask_type', 'x_policy_ref_datamask_FK_added_by_id');
call drop_table_column('x_policy_ref_datamask_type', 'added_by_id');
call drop_table_foreign_key('x_policy_ref_datamask_type', 'x_policy_ref_datamask_FK_upd_by_id');
call drop_table_column('x_policy_ref_datamask_type', 'upd_by_id');

drop procedure if exists drop_table_column;
drop procedure if exists drop_table_foreign_key;
