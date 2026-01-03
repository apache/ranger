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

DROP VIEW IF EXISTS `vx_principal`;

CREATE VIEW vx_principal as (SELECT u.user_name AS principal_name, 0 AS principal_type, u.status status, u.is_visible is_visible, u.other_attributes other_attributes, u.create_time create_time, u.update_time update_time, u.added_by_id added_by_id, u.upd_by_id upd_by_id FROM x_user u) UNION (SELECT g.group_name principal_name, 1 AS principal_type, g.status status, g.is_visible is_visible, g.other_attributes other_attributes, g.create_time create_time, g.update_time update_time, g.added_by_id added_by_id, g.upd_by_id upd_by_id FROM x_group g) UNION (SELECT r.name principal_name, 2 AS principal_name, 1 status, 1 is_visible, null other_attributes, r.create_time create_time, r.update_time update_time, r.added_by_id added_by_id, r.upd_by_id upd_by_id FROM x_role r);