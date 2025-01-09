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

drop procedure if exists create_index_for_x_trx_log;

delimiter ;;
create procedure create_index_for_x_trx_log() begin
if exists (SELECT * FROM INFORMATION_SCHEMA.STATISTICS WHERE table_schema=DATABASE() AND table_name='x_trx_log') then
  if not exists (SELECT * FROM INFORMATION_SCHEMA.STATISTICS WHERE table_schema=DATABASE() AND table_name='x_trx_log' AND index_name='x_trx_log_IDX_trx_id') then
    CREATE INDEX x_trx_log_IDX_trx_id ON x_trx_log(trx_id);
  end if;
end if;
end;;

delimiter ;
call create_index_for_x_trx_log();
