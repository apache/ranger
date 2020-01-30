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

CREATE OR REPLACE PROCEDURE spdroptablecolumn(TableName IN varchar2, ColumnName IN varchar2)
IS
	v_column_exists number := 0;
BEGIN
  select count(*) into v_column_exists from user_tab_cols where table_name = upper(TableName) and column_name = upper(ColumnName);
  if (v_column_exists > 0) then
    execute immediate 'ALTER TABLE ' || TableName || ' DROP COLUMN ' || ColumnName || ' CASCADE CONSTRAINTS';
    commit;
  end if;
END;
/

call spdroptablecolumn('x_policy_ref_resource', 'create_time');
call spdroptablecolumn('x_policy_ref_resource', 'update_time');
call spdroptablecolumn('x_policy_ref_resource', 'added_by_id');
call spdroptablecolumn('x_policy_ref_resource', 'upd_by_id');

call spdroptablecolumn('x_policy_ref_role', 'create_time');
call spdroptablecolumn('x_policy_ref_role', 'update_time');
call spdroptablecolumn('x_policy_ref_role', 'added_by_id');
call spdroptablecolumn('x_policy_ref_role', 'upd_by_id');

call spdroptablecolumn('x_policy_ref_group', 'create_time');
call spdroptablecolumn('x_policy_ref_group', 'update_time');
call spdroptablecolumn('x_policy_ref_group', 'added_by_id');
call spdroptablecolumn('x_policy_ref_group', 'upd_by_id');

call spdroptablecolumn('x_policy_ref_user', 'create_time');
call spdroptablecolumn('x_policy_ref_user', 'update_time');
call spdroptablecolumn('x_policy_ref_user', 'added_by_id');
call spdroptablecolumn('x_policy_ref_user', 'upd_by_id');

call spdroptablecolumn('x_policy_ref_access_type', 'create_time');
call spdroptablecolumn('x_policy_ref_access_type', 'update_time');
call spdroptablecolumn('x_policy_ref_access_type', 'added_by_id');
call spdroptablecolumn('x_policy_ref_access_type', 'upd_by_id');

call spdroptablecolumn('x_policy_ref_condition', 'create_time');
call spdroptablecolumn('x_policy_ref_condition', 'update_time');
call spdroptablecolumn('x_policy_ref_condition', 'added_by_id');
call spdroptablecolumn('x_policy_ref_condition', 'upd_by_id');

call spdroptablecolumn('x_policy_ref_datamask_type', 'create_time');
call spdroptablecolumn('x_policy_ref_datamask_type', 'update_time');
call spdroptablecolumn('x_policy_ref_datamask_type', 'added_by_id');
call spdroptablecolumn('x_policy_ref_datamask_type', 'upd_by_id');
