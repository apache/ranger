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



CREATE OR REPLACE FUNCTION getXportalUIdByLoginId(input_val varchar(100))
RETURNS bigint LANGUAGE SQL AS $$ SELECT x_portal_user.id FROM x_portal_user
WHERE x_portal_user.login_id = $1; $$;

CREATE OR REPLACE FUNCTION getModulesIdByName(input_val varchar(100))
RETURNS bigint LANGUAGE SQL AS $$ SELECT x_modules_master.id FROM x_modules_master
WHERE x_modules_master.module = $1; $$;



select 'delimiter start';
CREATE OR REPLACE FUNCTION add_gds_permissions()
RETURNS void AS $$
DECLARE
 v_column_exists integer := 0;
BEGIN
 select count(*) into v_column_exists from x_modules_master where module='Governed Data Sharing';
 IF v_column_exists = 0 THEN
   INSERT INTO x_modules_master(create_time,update_time,added_by_id,upd_by_id,module,url) VALUES(current_timestamp,current_timestamp,getXportalUIdByLoginId('admin'),getXportalUIdByLoginId('admin'),'Governed Data Sharing','');
 END IF;

 v_column_exists:=0;
 select count(*) into v_column_exists from x_user_module_perm where user_id=getXportalUIdByLoginId('admin') and module_id=getModulesIdByName('Governed Data Sharing');
 IF v_column_exists = 0 THEN
   INSERT INTO x_user_module_perm (user_id,module_id,create_time,update_time,added_by_id,upd_by_id,is_allowed) VALUES (getXportalUIdByLoginId('admin'),getModulesIdByName('Governed Data Sharing'),current_timestamp,current_timestamp,getXportalUIdByLoginId('admin'),getXportalUIdByLoginId('admin'),1);
 END IF;

 v_column_exists:=0;
 select count(*) into v_column_exists from x_user_module_perm where user_id=getXportalUIdByLoginId('rangerusersync') and module_id=getModulesIdByName('Governed Data Sharing');
 IF v_column_exists = 0 THEN
   INSERT INTO x_user_module_perm (user_id,module_id,create_time,update_time,added_by_id,upd_by_id,is_allowed) VALUES (getXportalUIdByLoginId('rangerusersync'),getModulesIdByName('Governed Data Sharing'),current_timestamp,current_timestamp,getXportalUIdByLoginId('admin'),getXportalUIdByLoginId('admin'),1);
 END IF;

 v_column_exists:=0;
 select count(*) into v_column_exists from x_user_module_perm where user_id=getXportalUIdByLoginId('rangertagsync') and module_id=getModulesIdByName('Governed Data Sharing');
 IF v_column_exists = 0 THEN
   INSERT INTO x_user_module_perm (user_id,module_id,create_time,update_time,added_by_id,upd_by_id,is_allowed) VALUES (getXportalUIdByLoginId('rangertagsync'),getModulesIdByName('Governed Data Sharing'),current_timestamp,current_timestamp,getXportalUIdByLoginId('admin'),getXportalUIdByLoginId('admin'),1);
 END IF;

END;
$$ LANGUAGE plpgsql;
select 'delimiter end';

select add_gds_permissions();
select 'delimiter end';

commit;
