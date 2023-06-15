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

IF (OBJECT_ID('x_sz_ref_admin_role_FK_added_by_id') IS NOT NULL)
BEGIN
    ALTER TABLE [dbo].[x_security_zone_ref_role] DROP CONSTRAINT x_sz_ref_role_FK_added_by_id
END
IF (OBJECT_ID('x_sz_ref_role_FK_upd_by_id') IS NOT NULL)
BEGIN
    ALTER TABLE [dbo].[x_security_zone_ref_role] DROP CONSTRAINT x_sz_ref_role_FK_upd_by_id
END
IF (OBJECT_ID('x_sz_ref_role_FK_zone_id') IS NOT NULL)
BEGIN
    ALTER TABLE [dbo].[x_security_zone_ref_role] DROP CONSTRAINT x_sz_ref_role_FK_zone_id
END
IF (OBJECT_ID('x_sz_ref_role_FK_role_id') IS NOT NULL)
BEGIN
    ALTER TABLE [dbo].[x_security_zone_ref_role] DROP CONSTRAINT x_sz_ref_role_FK_role_id
END

IF (OBJECT_ID('x_security_zone_ref_role') IS NOT NULL)
BEGIN
    DROP TABLE [dbo].[x_security_zone_ref_role]
END

CREATE TABLE [dbo].[x_security_zone_ref_role](
        [id] [bigint] IDENTITY(1,1) NOT NULL,
        [create_time] [datetime2] DEFAULT NULL NULL,
        [update_time] [datetime2] DEFAULT NULL NULL,
        [added_by_id] [bigint] DEFAULT NULL NULL,
        [upd_by_id] [bigint] DEFAULT NULL NULL,
        [zone_id] [bigint] DEFAULT NULL NULL,
        [role_id] [bigint] DEFAULT NULL NULL,
        [role_name] [nvarchar](767) DEFAULT NULL NULL
        PRIMARY KEY CLUSTERED
(
        [id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY],
) ON [PRIMARY]
GO
ALTER TABLE [dbo].[x_security_zone_ref_role] WITH CHECK ADD CONSTRAINT [x_sz_ref_role_FK_added_by_id] FOREIGN KEY([added_by_id]) REFERENCES [dbo].[x_portal_user] ([id])
GO
ALTER TABLE [dbo].[x_security_zone_ref_role] WITH CHECK ADD CONSTRAINT [x_sz_ref_role_FK_upd_by_id] FOREIGN KEY([upd_by_id]) REFERENCES [dbo].[x_portal_user] ([id])
GO
ALTER TABLE [dbo].[x_security_zone_ref_role] WITH CHECK ADD CONSTRAINT [x_sz_ref_role_FK_zone_id] FOREIGN KEY([zone_id]) REFERENCES [dbo].[x_security_zone] ([id])
GO
ALTER TABLE [dbo].[x_security_zone_ref_role] WITH CHECK ADD CONSTRAINT [x_sz_ref_role_FK_role_id] FOREIGN KEY([role_id]) REFERENCES [dbo].[x_role] ([id])
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
SET ANSI_PADDING ON

exit