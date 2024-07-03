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



select 'delimiter start';
CREATE OR REPLACE FUNCTION add_gds_version_x_service_version_info()
RETURNS void AS $$
DECLARE
 v_column_exists integer := 0;
BEGIN
 select count(*) into v_column_exists from pg_attribute where attrelid in(select oid from pg_class where relname='x_service_version_info') and attname='gds_version';
 IF v_column_exists = 0 THEN
  ALTER TABLE x_service_version_info ADD COLUMN gds_version bigint NOT NULL DEFAULT '0';
 END IF;

END;
$$ LANGUAGE plpgsql;
select 'delimiter end';

select add_gds_version_x_service_version_info();
select 'delimiter end';
commit;



select 'delimiter start';
CREATE OR REPLACE FUNCTION add_gds_update_time_x_service_version_info()
RETURNS void AS $$
DECLARE
 v_column_exists integer := 0;
BEGIN
 select count(*) into v_column_exists from pg_attribute where attrelid in(select oid from pg_class where relname='x_service_version_info') and attname='gds_update_time';
 IF v_column_exists = 0 THEN
  ALTER TABLE x_service_version_info ADD COLUMN gds_update_time TIMESTAMP DEFAULT NULL;
 END IF;

END;
$$ LANGUAGE plpgsql;
select 'delimiter end';

select add_gds_update_time_x_service_version_info();
select 'delimiter end';
commit;




