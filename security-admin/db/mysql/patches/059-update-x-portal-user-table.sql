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

drop procedure if exists add_old_passwords_column_to_x_portal_user_table;

delimiter ;;
create procedure add_old_passwords_column_to_x_portal_user_table() begin

 if exists (select * from information_schema.columns where table_schema=database() and table_name = 'x_portal_user') then
  if not exists (select * from information_schema.columns where table_schema=database() and table_name = 'x_portal_user' and column_name = 'old_passwords') then
    ALTER TABLE  `x_portal_user` ADD `old_passwords` text DEFAULT NULL;
    ALTER TABLE  `x_portal_user` ADD `password_updated_time` datetime DEFAULT NULL;
  end if;
 end if;
end;;

delimiter ;
call add_old_passwords_column_to_x_portal_user_table();

drop procedure if exists add_old_passwords_column_to_x_portal_user_table;