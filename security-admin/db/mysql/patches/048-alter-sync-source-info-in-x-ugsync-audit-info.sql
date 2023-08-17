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

drop procedure if exists alter_table_x_ugsync_audit_info;

delimiter ;;
create procedure alter_table_x_ugsync_audit_info() begin

if exists (select * from information_schema.columns where table_schema=database() and table_name = 'x_ugsync_audit_info' and column_name = 'sync_source_info' and data_type='varchar') then
	ALTER TABLE `x_ugsync_audit_info` MODIFY `sync_source_info` MEDIUMTEXT NOT NULL;
 end if;
end;;

delimiter ;
call alter_table_x_ugsync_audit_info();