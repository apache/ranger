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

DROP VIEW IF EXISTS vx_security_zone_user;
CREATE VIEW vx_security_zone_user AS
(
    SELECT DISTINCT
        sz.id AS zone_id,
        sz.name AS zone_name,
        refu.user_id AS user_id,
        0 AS access_type
    FROM x_security_zone sz
    INNER JOIN x_security_zone_ref_user refu ON sz.id = refu.zone_id
    WHERE refu.user_id IS NOT NULL
)
UNION
(
    SELECT DISTINCT
        sz.id AS zone_id,
        sz.name AS zone_name,
        gu.user_id AS user_id,
        1 AS access_type
    FROM x_security_zone sz
    INNER JOIN x_security_zone_ref_group refg ON sz.id = refg.zone_id
    INNER JOIN x_group_users gu ON refg.group_id = gu.p_group_id
    WHERE gu.user_id IS NOT NULL
)
UNION
(
    SELECT DISTINCT
        sz.id AS zone_id,
        sz.name AS zone_name,
        rru.user_id AS user_id,
        2 AS access_type
    FROM x_security_zone sz
    INNER JOIN x_security_zone_ref_role refr ON sz.id = refr.zone_id
    INNER JOIN x_role_ref_user rru ON refr.role_id = rru.role_id
    WHERE rru.user_id IS NOT NULL
)
UNION
(
    SELECT DISTINCT
        sz.id AS zone_id,
        sz.name AS zone_name,
        gu.user_id AS user_id,
        3 AS access_type
    FROM x_security_zone sz
    INNER JOIN x_security_zone_ref_role refr ON sz.id = refr.zone_id
    INNER JOIN x_role_ref_group rrg ON refr.role_id = rrg.role_id
    INNER JOIN x_group_users gu ON rrg.group_id = gu.p_group_id
    WHERE gu.user_id IS NOT NULL
)
UNION
(
    SELECT DISTINCT
        sz.id AS zone_id,
        sz.name AS zone_name,
        u.id AS user_id,
        4 AS access_type
    FROM x_security_zone sz
    INNER JOIN x_security_zone_ref_group refg ON sz.id = refg.zone_id
    CROSS JOIN x_user u
    WHERE refg.group_name = 'public'
);
