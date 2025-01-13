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
-- sync_source_info CLOB NOT NULL,

DECLARE
	v_index_exists number:=0;
	v_table_exists number := 0;
BEGIN
	SELECT COUNT(*) INTO v_table_exists FROM USER_TABLES WHERE TABLE_NAME = upper('x_trx_log');
	IF (v_table_exists > 0) THEN
		SELECT COUNT(*) INTO v_index_exists FROM USER_INDEXES WHERE INDEX_NAME = upper('x_trx_log_IDX_trx_id') AND TABLE_NAME= upper('x_trx_log');
		IF (v_index_exists = 0) THEN
			execute IMMEDIATE 'CREATE INDEX x_trx_log_IDX_trx_id ON x_trx_log(trx_id)';
			commit;
		END IF;
	END IF;
END;/
