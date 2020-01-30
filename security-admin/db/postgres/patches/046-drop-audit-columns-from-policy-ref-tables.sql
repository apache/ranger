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

ALTER TABLE x_policy_ref_resource DROP COLUMN IF EXISTS create_time CASCADE;
ALTER TABLE x_policy_ref_resource DROP COLUMN IF EXISTS update_time CASCADE;
ALTER TABLE x_policy_ref_resource DROP COLUMN IF EXISTS added_by_id CASCADE;
ALTER TABLE x_policy_ref_resource DROP COLUMN IF EXISTS upd_by_id CASCADE;

ALTER TABLE x_policy_ref_role DROP COLUMN IF EXISTS create_time CASCADE;
ALTER TABLE x_policy_ref_role DROP COLUMN IF EXISTS update_time CASCADE;
ALTER TABLE x_policy_ref_role DROP COLUMN IF EXISTS added_by_id CASCADE;
ALTER TABLE x_policy_ref_role DROP COLUMN IF EXISTS upd_by_id CASCADE;

ALTER TABLE x_policy_ref_group DROP COLUMN IF EXISTS create_time CASCADE;
ALTER TABLE x_policy_ref_group DROP COLUMN IF EXISTS update_time CASCADE;
ALTER TABLE x_policy_ref_group DROP COLUMN IF EXISTS added_by_id CASCADE;
ALTER TABLE x_policy_ref_group DROP COLUMN IF EXISTS upd_by_id CASCADE;

ALTER TABLE x_policy_ref_user DROP COLUMN IF EXISTS create_time CASCADE;
ALTER TABLE x_policy_ref_user DROP COLUMN IF EXISTS update_time CASCADE;
ALTER TABLE x_policy_ref_user DROP COLUMN IF EXISTS added_by_id CASCADE;
ALTER TABLE x_policy_ref_user DROP COLUMN IF EXISTS upd_by_id CASCADE;

ALTER TABLE x_policy_ref_access_type DROP COLUMN IF EXISTS create_time CASCADE;
ALTER TABLE x_policy_ref_access_type DROP COLUMN IF EXISTS update_time CASCADE;
ALTER TABLE x_policy_ref_access_type DROP COLUMN IF EXISTS added_by_id CASCADE;
ALTER TABLE x_policy_ref_access_type DROP COLUMN IF EXISTS upd_by_id CASCADE;

ALTER TABLE x_policy_ref_condition DROP COLUMN IF EXISTS create_time CASCADE;
ALTER TABLE x_policy_ref_condition DROP COLUMN IF EXISTS update_time CASCADE;
ALTER TABLE x_policy_ref_condition DROP COLUMN IF EXISTS added_by_id CASCADE;
ALTER TABLE x_policy_ref_condition DROP COLUMN IF EXISTS upd_by_id CASCADE;

ALTER TABLE x_policy_ref_datamask_type DROP COLUMN IF EXISTS create_time CASCADE;
ALTER TABLE x_policy_ref_datamask_type DROP COLUMN IF EXISTS update_time CASCADE;
ALTER TABLE x_policy_ref_datamask_type DROP COLUMN IF EXISTS added_by_id CASCADE;
ALTER TABLE x_policy_ref_datamask_type DROP COLUMN IF EXISTS upd_by_id CASCADE;
