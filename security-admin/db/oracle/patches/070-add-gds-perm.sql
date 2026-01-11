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

CREATE OR REPLACE FUNCTION getXportalUIdByLoginId(input_val IN VARCHAR2)
RETURN NUMBER iS
BEGIN
DECLARE
myid Number := 0;
begin
    SELECT x_portal_user.id into myid FROM x_portal_user
    WHERE x_portal_user.login_id=input_val;
    RETURN myid;
end;
END;/


CREATE OR REPLACE FUNCTION getModulesIdByName(inputval IN VARCHAR2)
RETURN NUMBER is
BEGIN
Declare
myid Number := 0;
begin
   SELECT id into myid FROM x_modules_master
   WHERE MODULE = inputval;
   RETURN myid;
end;
END;/

DECLARE
        v_count number:=0;
BEGIN
        select count(*) into v_count from x_modules_master where module='Governed Data Sharing';
        if (v_count = 0) then
            INSERT INTO x_modules_master VALUES(X_MODULES_MASTER_SEQ.NEXTVAL,sys_extract_utc(systimestamp),sys_extract_utc(systimestamp),getXportalUIdByLoginId('admin'),getXportalUIdByLoginId('admin'),'Governed Data Sharing','');
        end if;
        v_count:=0;
        select count(*) into v_count from x_user_module_perm where user_id=getXportalUIdByLoginId('admin') and module_id=getModulesIdByName('Governed Data Sharing');
        if (v_count = 0) then
            INSERT INTO x_user_module_perm (id,user_id,module_id,create_time,update_time,added_by_id,upd_by_id,is_allowed) VALUES (X_USER_MODULE_PERM_SEQ.nextval,getXportalUIdByLoginId('admin'),getModulesIdByName('Governed Data Sharing'),sys_extract_utc(systimestamp),sys_extract_utc(systimestamp),getXportalUIdByLoginId('admin'),getXportalUIdByLoginId('admin'),1);
        end if;
        v_count:=0;
        select count(*) into v_count from x_user_module_perm where user_id=getXportalUIdByLoginId('rangerusersync') and module_id=getModulesIdByName('Governed Data Sharing');
        if (v_count = 0) then
            INSERT INTO x_user_module_perm (id,user_id,module_id,create_time,update_time,added_by_id,upd_by_id,is_allowed) VALUES (X_USER_MODULE_PERM_SEQ.nextval,getXportalUIdByLoginId('rangerusersync'),getModulesIdByName('Governed Data Sharing'),sys_extract_utc(systimestamp),sys_extract_utc(systimestamp),getXportalUIdByLoginId('admin'),getXportalUIdByLoginId('admin'),1);
        end if;
        v_count:=0;
        select count(*) into v_count from x_user_module_perm where user_id=getXportalUIdByLoginId('rangertagsync') and module_id=getModulesIdByName('Governed Data Sharing');
        if (v_count = 0) then
            INSERT INTO x_user_module_perm (id,user_id,module_id,create_time,update_time,added_by_id,upd_by_id,is_allowed) VALUES (X_USER_MODULE_PERM_SEQ.nextval,getXportalUIdByLoginId('rangertagsync'),getModulesIdByName('Governed Data Sharing'),sys_extract_utc(systimestamp),sys_extract_utc(systimestamp),getXportalUIdByLoginId('admin'),getXportalUIdByLoginId('admin'),1);
        end if;
        commit;
END;/
