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
CREATE OR REPLACE FUNCTION alter_table_x_ugsync_audit_info()
RETURNS void AS $$
DECLARE
	v_column_exists integer := 0;
BEGIN
	select count(*) into v_column_exists from pg_attribute where attrelid in(select oid from pg_class where relname='x_ugsync_audit_info') and attname='sync_source_info' and atttypid = (select oid from pg_type where typname='varchar');
	IF v_column_exists = 1 THEN
		ALTER TABLE x_ugsync_audit_info alter column sync_source_info type TEXT;
	END IF;
END;
$$ LANGUAGE plpgsql;
select 'delimiter end';

select alter_table_x_ugsync_audit_info();
select 'delimiter end';