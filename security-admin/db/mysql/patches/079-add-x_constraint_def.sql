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

CREATE TABLE IF NOT EXISTS `x_constraint_def` (
`id` bigint(20) NOT NULL AUTO_INCREMENT,
`guid` varchar(1024) DEFAULT NULL,
`create_time` datetime DEFAULT NULL,
`update_time` datetime DEFAULT NULL,
`added_by_id` bigint(20) DEFAULT NULL,
`upd_by_id` bigint(20) DEFAULT NULL,
`def_id` bigint(20) NOT NULL,
`item_id` bigint(20) NOT NULL,
`name` varchar(1024) DEFAULT NULL,
`label` varchar(1024) DEFAULT NULL,
`description` varchar(1024) DEFAULT NULL,
`rb_key_label` varchar(1024) DEFAULT NULL,
`rb_key_description` varchar(1024) DEFAULT NULL,
`resources` mediumtext DEFAULT NULL,
`access_types` mediumtext DEFAULT NULL,
`parameters` mediumtext DEFAULT NULL,
`options` mediumtext DEFAULT NULL,
`sort_order` int DEFAULT 0,
primary key (`id`),
CONSTRAINT `x_constraint_def_FK_defid` FOREIGN KEY (`def_id`) REFERENCES `x_service_def` (`id`),
CONSTRAINT `x_constraint_def_FK_added_by_id` FOREIGN KEY (`added_by_id`) REFERENCES `x_portal_user` (`id`),
CONSTRAINT `x_constraint_def_FK_upd_by_id` FOREIGN KEY (`upd_by_id`) REFERENCES `x_portal_user` (`id`)
)ROW_FORMAT=DYNAMIC;

drop procedure if exists create_index_x_constraint_def;

delimiter ;;
create procedure create_index_x_constraint_def() begin
if exists (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE table_schema=DATABASE() AND table_name='x_constraint_def') then
	if not exists (SELECT * FROM INFORMATION_SCHEMA.STATISTICS WHERE table_schema=DATABASE() AND table_name='x_constraint_def' AND index_name='x_constraint_def_IDX_def_id') then
		CREATE INDEX x_constraint_def_IDX_def_id ON x_constraint_def(def_id);
	end if;
end if;
end;;

delimiter ;
call create_index_x_constraint_def();
drop procedure if exists create_index_x_constraint_def;
