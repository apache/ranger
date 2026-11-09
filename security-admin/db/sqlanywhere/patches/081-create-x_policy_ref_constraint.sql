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

IF NOT EXISTS(SELECT * FROM SYS.SYSTABLE WHERE table_name = 'x_policy_ref_constraint') THEN
	CREATE TABLE dbo.x_policy_ref_constraint (
		id bigint IDENTITY NOT NULL,
		policy_id bigint NOT NULL,
		service_def_id bigint NOT NULL,
		constraint_name varchar(255) NOT NULL,
		CONSTRAINT x_policy_ref_cnst_PK_id PRIMARY KEY CLUSTERED(id),
		CONSTRAINT x_p_ref_cnst_UK_polId_cName UNIQUE NONCLUSTERED (policy_id, constraint_name)
	);
	ALTER TABLE dbo.x_policy_ref_constraint ADD CONSTRAINT x_policy_ref_constraint_FK_policy_id FOREIGN KEY (policy_id) REFERENCES dbo.x_policy (id);
	ALTER TABLE dbo.x_policy_ref_constraint ADD CONSTRAINT x_policy_ref_constraint_FK_svc_def_id FOREIGN KEY (service_def_id) REFERENCES dbo.x_service_def (id);
END IF;
GO
exit
