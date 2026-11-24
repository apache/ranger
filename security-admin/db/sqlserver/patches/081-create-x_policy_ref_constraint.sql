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

GO
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'x_policy_ref_constraint')
BEGIN
	CREATE TABLE [dbo].[x_policy_ref_constraint] (
		[id] [bigint] IDENTITY (1, 1) NOT NULL,
		[policy_id] [bigint] NOT NULL,
		[service_def_id] [bigint] NOT NULL,
		[constraint_name] [varchar](255) NOT NULL,
		PRIMARY KEY CLUSTERED ([id] ASC),
		CONSTRAINT [x_policy_ref_constraint$x_policy_ref_constraint_UK] UNIQUE NONCLUSTERED ([policy_id] ASC, [constraint_name] ASC)
	) ON [PRIMARY];
	ALTER TABLE [dbo].[x_policy_ref_constraint] WITH CHECK ADD CONSTRAINT [x_policy_ref_constraint_FK_policy_id] FOREIGN KEY ([policy_id]) REFERENCES [dbo].[x_policy] ([id]);
	ALTER TABLE [dbo].[x_policy_ref_constraint] WITH CHECK ADD CONSTRAINT [x_policy_ref_constraint_FK_svc_def_id] FOREIGN KEY ([service_def_id]) REFERENCES [dbo].[x_service_def] ([id]);
END
GO
exit
