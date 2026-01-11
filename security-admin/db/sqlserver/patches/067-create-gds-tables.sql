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

IF (OBJECT_ID('x_gds_dataset_FK_added_by_id') IS NOT NULL)
BEGIN
    ALTER TABLE [dbo].[x_gds_dataset] DROP CONSTRAINT x_gds_dataset_FK_added_by_id
END
IF (OBJECT_ID('x_gds_dataset_FK_upd_by_id') IS NOT NULL)
BEGIN
    ALTER TABLE [dbo].[x_gds_dataset] DROP CONSTRAINT x_gds_dataset_FK_upd_by_id
END
IF (OBJECT_ID('x_gds_project_FK_added_by_id') IS NOT NULL)
BEGIN
    ALTER TABLE [dbo].[x_gds_project] DROP CONSTRAINT x_gds_project_FK_added_by_id
END
IF (OBJECT_ID('x_gds_project_FK_upd_by_id') IS NOT NULL)
BEGIN
    ALTER TABLE [dbo].[x_gds_project] DROP CONSTRAINT x_gds_project_FK_upd_by_id
END
IF (OBJECT_ID('x_gds_data_share_FK_added_by_id') IS NOT NULL)
BEGIN
    ALTER TABLE [dbo].[x_gds_data_share] DROP CONSTRAINT x_gds_data_share_FK_added_by_id
END
IF (OBJECT_ID('x_gds_data_share_FK_upd_by_id') IS NOT NULL)
BEGIN
    ALTER TABLE [dbo].[x_gds_data_share] DROP CONSTRAINT x_gds_data_share_FK_upd_by_id
END
IF (OBJECT_ID('x_gds_data_share_FK_service_id') IS NOT NULL)
BEGIN
    ALTER TABLE [dbo].[x_gds_data_share] DROP CONSTRAINT x_gds_data_share_FK_service_id
END
IF (OBJECT_ID('x_gds_data_share_FK_zone_id') IS NOT NULL)
BEGIN
    ALTER TABLE [dbo].[x_gds_data_share] DROP CONSTRAINT x_gds_data_share_FK_zone_id
END
IF (OBJECT_ID('x_gds_shared_resource_FK_added_by_id') IS NOT NULL)
BEGIN
    ALTER TABLE [dbo].[x_gds_shared_resource] DROP CONSTRAINT x_gds_shared_resource_FK_added_by_id
END
IF (OBJECT_ID('x_gds_shared_resource_FK_upd_by_id') IS NOT NULL)
BEGIN
    ALTER TABLE [dbo].[x_gds_shared_resource] DROP CONSTRAINT x_gds_shared_resource_FK_upd_by_id
END
IF (OBJECT_ID('x_gds_shared_resource_FK_data_share_id') IS NOT NULL)
BEGIN
    ALTER TABLE [dbo].[x_gds_shared_resource] DROP CONSTRAINT x_gds_shared_resource_FK_data_share_id
END
IF (OBJECT_ID('x_gds_dshid_FK_added_by_id') IS NOT NULL)
BEGIN
    ALTER TABLE [dbo].[x_gds_data_share_in_dataset] DROP CONSTRAINT x_gds_dshid_FK_added_by_id
END
IF (OBJECT_ID('x_gds_dshid_FK_upd_by_id') IS NOT NULL)
BEGIN
    ALTER TABLE [dbo].[x_gds_data_share_in_dataset] DROP CONSTRAINT x_gds_dshid_FK_upd_by_id
END
IF (OBJECT_ID('x_gds_dshid_FK_data_share_id') IS NOT NULL)
BEGIN
    ALTER TABLE [dbo].[x_gds_data_share_in_dataset] DROP CONSTRAINT x_gds_dshid_FK_data_share_id
END
IF (OBJECT_ID('x_gds_dshid_FK_dataset_id') IS NOT NULL)
BEGIN
    ALTER TABLE [dbo].[x_gds_data_share_in_dataset] DROP CONSTRAINT x_gds_dshid_FK_dataset_id
END
IF (OBJECT_ID('x_gds_dshid_FK_approver_id') IS NOT NULL)
BEGIN
    ALTER TABLE [dbo].[x_gds_data_share_in_dataset] DROP CONSTRAINT x_gds_dshid_FK_approver_id
END
IF (OBJECT_ID('x_gds_dip_FK_added_by_id') IS NOT NULL)
BEGIN
    ALTER TABLE [dbo].[x_gds_dataset_in_project] DROP CONSTRAINT x_gds_dip_FK_added_by_id
END
IF (OBJECT_ID('x_gds_dip_FK_upd_by_id') IS NOT NULL)
BEGIN
    ALTER TABLE [dbo].[x_gds_dataset_in_project] DROP CONSTRAINT x_gds_dip_FK_upd_by_id
END
IF (OBJECT_ID('x_gds_dip_FK_dataset_id') IS NOT NULL)
BEGIN
    ALTER TABLE [dbo].[x_gds_dataset_in_project] DROP CONSTRAINT x_gds_dip_FK_dataset_id
END
IF (OBJECT_ID('x_gds_dip_FK_project_id') IS NOT NULL)
BEGIN
    ALTER TABLE [dbo].[x_gds_dataset_in_project] DROP CONSTRAINT x_gds_dip_FK_project_id
END
IF (OBJECT_ID('x_gds_dip_FK_approver_id') IS NOT NULL)
BEGIN
    ALTER TABLE [dbo].[x_gds_dataset_in_project] DROP CONSTRAINT x_gds_dip_FK_approver_id
END
IF (OBJECT_ID('x_gds_dpm_FK_dataset_id') IS NOT NULL)
BEGIN
    ALTER TABLE [dbo].[x_gds_dataset_policy_map] DROP CONSTRAINT x_gds_dpm_FK_dataset_id
END
IF (OBJECT_ID('x_gds_dpm_FK_policy_id') IS NOT NULL)
BEGIN
    ALTER TABLE [dbo].[x_gds_dataset_policy_map] DROP CONSTRAINT x_gds_dpm_FK_policy_id
END
IF (OBJECT_ID('x_gds_ppm_FK_project_id') IS NOT NULL)
BEGIN
    ALTER TABLE [dbo].[x_gds_project_policy_map] DROP CONSTRAINT x_gds_ppm_FK_project_id
END
IF (OBJECT_ID('x_gds_ppm_FK_policy_id') IS NOT NULL)
BEGIN
    ALTER TABLE [dbo].[x_gds_project_policy_map] DROP CONSTRAINT x_gds_ppm_FK_policy_id
END
IF (OBJECT_ID('x_gds_dataset') IS NOT NULL)
BEGIN
    DROP TABLE [dbo].[x_gds_dataset]
END
IF (OBJECT_ID('x_gds_project') IS NOT NULL)
BEGIN
    DROP TABLE [dbo].[x_gds_project]
END
IF (OBJECT_ID('x_gds_data_share') IS NOT NULL)
BEGIN
    DROP TABLE [dbo].[x_gds_data_share]
END
IF (OBJECT_ID('x_gds_shared_resource') IS NOT NULL)
BEGIN
    DROP TABLE [dbo].[x_gds_shared_resource]
END
IF (OBJECT_ID('x_gds_data_share_in_dataset') IS NOT NULL)
BEGIN
    DROP TABLE [dbo].[x_gds_data_share_in_dataset]
END
IF (OBJECT_ID('x_gds_dataset_in_project') IS NOT NULL)
BEGIN
    DROP TABLE [dbo].[x_gds_dataset_in_project]
END
IF (OBJECT_ID('x_gds_dataset_policy_map') IS NOT NULL)
BEGIN
    DROP TABLE [dbo].[x_gds_dataset_policy_map]
END
IF (OBJECT_ID('x_gds_project_policy_map') IS NOT NULL)
BEGIN
    DROP TABLE [dbo].[x_gds_project_policy_map]
END


SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
SET ANSI_PADDING ON
CREATE TABLE [dbo].[x_gds_dataset](
        [id]                [bigint] IDENTITY(1,1) NOT NULL,
        [guid]              [varchar](64) NOT NULL,
        [create_time]       [datetime2] DEFAULT NULL NULL,
        [update_time]       [datetime2] DEFAULT NULL NULL,
        [added_by_id]       [bigint] DEFAULT NULL NULL,
        [upd_by_id]         [bigint] DEFAULT NULL NULL,
        [version]           [bigint] NOT NULL DEFAULT 1,
        [is_enabled]        [tinyint] DEFAULT 1 NOT NULL,
        [name]              [varchar](512) NOT NULL,
        [description]       [nvarchar](max) DEFAULT NULL NULL,
        [acl]               [nvarchar](max) DEFAULT NULL NULL,
        [terms_of_use]      [nvarchar](max) DEFAULT NULL NULL,
        [options]           [nvarchar](max) DEFAULT NULL NULL,
        [additional_info]   [nvarchar](max) DEFAULT NULL NULL,
PRIMARY KEY CLUSTERED
(
        [id] ASC
)WITH (PAD_INDEX = OFF,STATISTICS_NORECOMPUTE = OFF,IGNORE_DUP_KEY = OFF,ALLOW_ROW_LOCKS = ON,ALLOW_PAGE_LOCKS = ON) ON [PRIMARY],
 CONSTRAINT [x_gds_dataset$x_gds_dataset_UK_name] UNIQUE NONCLUSTERED
(
        [name] ASC
)WITH (PAD_INDEX = OFF,STATISTICS_NORECOMPUTE = OFF,IGNORE_DUP_KEY = OFF,ALLOW_ROW_LOCKS = ON,ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
ALTER TABLE [dbo].[x_gds_dataset]  WITH CHECK ADD  CONSTRAINT [x_gds_dataset_FK_added_by_id] FOREIGN KEY([added_by_id]) REFERENCES [dbo].[x_portal_user] ([id])
ALTER TABLE [dbo].[x_gds_dataset]  WITH CHECK ADD  CONSTRAINT [x_gds_dataset_FK_upd_by_id] FOREIGN KEY([upd_by_id]) REFERENCES [dbo].[x_portal_user] ([id])
GO
CREATE NONCLUSTERED INDEX [x_gds_dataset_guid] ON [x_gds_dataset]
(
   [guid] ASC
)
WITH (SORT_IN_TEMPDB = OFF,DROP_EXISTING = OFF,IGNORE_DUP_KEY = OFF,ONLINE = OFF) ON [PRIMARY]
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
SET ANSI_PADDING ON
CREATE TABLE [dbo].[x_gds_project](
        [id]                [bigint] IDENTITY(1,1) NOT NULL,
        [guid]              [varchar](64) NOT NULL,
        [create_time]       [datetime2] DEFAULT NULL NULL,
        [update_time]       [datetime2] DEFAULT NULL NULL,
        [added_by_id]       [bigint] DEFAULT NULL NULL,
        [upd_by_id]         [bigint] DEFAULT NULL NULL,
        [version]           [bigint] NOT NULL DEFAULT 1,
        [is_enabled]        [tinyint] DEFAULT 1 NOT NULL,
        [name]              [varchar](512) NOT NULL,
        [description]       [nvarchar](max) DEFAULT NULL NULL,
        [acl]               [nvarchar](max) DEFAULT NULL NULL,
        [terms_of_use]      [nvarchar](max) DEFAULT NULL NULL,
        [options]           [nvarchar](max) DEFAULT NULL NULL,
        [additional_info]   [nvarchar](max) DEFAULT NULL NULL,
PRIMARY KEY CLUSTERED
(
        [id] ASC
)WITH (PAD_INDEX = OFF,STATISTICS_NORECOMPUTE = OFF,IGNORE_DUP_KEY = OFF,ALLOW_ROW_LOCKS = ON,ALLOW_PAGE_LOCKS = ON) ON [PRIMARY],
 CONSTRAINT [x_gds_project$x_gds_project_UK_name] UNIQUE NONCLUSTERED
(
        [name] ASC
)WITH (PAD_INDEX = OFF,STATISTICS_NORECOMPUTE = OFF,IGNORE_DUP_KEY = OFF,ALLOW_ROW_LOCKS = ON,ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
ALTER TABLE [dbo].[x_gds_project]  WITH CHECK ADD  CONSTRAINT [x_gds_project_FK_added_by_id] FOREIGN KEY([added_by_id]) REFERENCES [dbo].[x_portal_user] ([id])
ALTER TABLE [dbo].[x_gds_project]  WITH CHECK ADD  CONSTRAINT [x_gds_project_FK_upd_by_id] FOREIGN KEY([upd_by_id]) REFERENCES [dbo].[x_portal_user] ([id])
GO
CREATE NONCLUSTERED INDEX [x_gds_project_guid] ON [x_gds_project]
(
   [guid] ASC
)
WITH (SORT_IN_TEMPDB = OFF,DROP_EXISTING = OFF,IGNORE_DUP_KEY = OFF,ONLINE = OFF) ON [PRIMARY]
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
SET ANSI_PADDING ON
CREATE TABLE [dbo].[x_gds_data_share](
        [id]                   [bigint] IDENTITY(1,1) NOT NULL,
        [guid]                 [varchar](64) NOT NULL,
        [create_time]          [datetime2] DEFAULT NULL NULL,
        [update_time]          [datetime2] DEFAULT NULL NULL,
        [added_by_id]          [bigint] DEFAULT NULL NULL,
        [upd_by_id]            [bigint] DEFAULT NULL NULL,
        [version]              [bigint] NOT NULL DEFAULT 1,
        [is_enabled]           [tinyint] DEFAULT 1 NOT NULL,
        [name]                 [varchar](512) NOT NULL,
        [description]          [nvarchar](max) DEFAULT NULL  NULL,
        [acl]                  [nvarchar](max) NOT NULL,
        [service_id]           [bigint] NOT NULL,
        [zone_id]              [bigint] NOT NULL,
        [condition_expr]       [nvarchar](max) DEFAULT NULL NULL,
        [default_access_types] [nvarchar](max) DEFAULT NULL NULL,
        [default_tag_masks]    [nvarchar](max) DEFAULT NULL NULL,
        [terms_of_use]         [nvarchar](max) DEFAULT NULL NULL,
        [options]              [nvarchar](max) DEFAULT NULL NULL,
        [additional_info]      [nvarchar](max) DEFAULT NULL NULL,
PRIMARY KEY CLUSTERED
(
        [id] ASC
)WITH (PAD_INDEX = OFF,STATISTICS_NORECOMPUTE = OFF,IGNORE_DUP_KEY = OFF,ALLOW_ROW_LOCKS = ON,ALLOW_PAGE_LOCKS = ON) ON [PRIMARY],
 CONSTRAINT [x_gds_data_share$x_gds_data_share_UK_name] UNIQUE NONCLUSTERED
(
        [service_id] ASC, [zone_id] ASC, [name] ASC
)WITH (PAD_INDEX = OFF,STATISTICS_NORECOMPUTE = OFF,IGNORE_DUP_KEY = OFF,ALLOW_ROW_LOCKS = ON,ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
ALTER TABLE [dbo].[x_gds_data_share]  WITH CHECK ADD  CONSTRAINT [x_gds_data_share_FK_added_by_id] FOREIGN KEY([added_by_id]) REFERENCES [dbo].[x_portal_user] ([id])
ALTER TABLE [dbo].[x_gds_data_share]  WITH CHECK ADD  CONSTRAINT [x_gds_data_share_FK_upd_by_id] FOREIGN KEY([upd_by_id]) REFERENCES [dbo].[x_portal_user] ([id])
ALTER TABLE [dbo].[x_gds_data_share]  WITH CHECK ADD  CONSTRAINT [x_gds_data_share_FK_service_id] FOREIGN KEY([service_id]) REFERENCES [dbo].[x_service] ([id])
ALTER TABLE [dbo].[x_gds_data_share]  WITH CHECK ADD  CONSTRAINT [x_gds_data_share_FK_zone_id] FOREIGN KEY([zone_id]) REFERENCES [dbo].[x_security_zone] ([id])
GO
CREATE NONCLUSTERED INDEX [x_gds_data_share_guid] ON [x_gds_data_share]
(
   [guid] ASC
)
WITH (SORT_IN_TEMPDB = OFF,DROP_EXISTING = OFF,IGNORE_DUP_KEY = OFF,ONLINE = OFF) ON [PRIMARY]
CREATE NONCLUSTERED INDEX [x_gds_data_share_service_id] ON [x_gds_data_share]
(
   [service_id] ASC
)
WITH (SORT_IN_TEMPDB = OFF,DROP_EXISTING = OFF,IGNORE_DUP_KEY = OFF,ONLINE = OFF) ON [PRIMARY]
CREATE NONCLUSTERED INDEX [x_gds_data_share_zone_id] ON [x_gds_data_share]
(
   [zone_id] ASC
)
WITH (SORT_IN_TEMPDB = OFF,DROP_EXISTING = OFF,IGNORE_DUP_KEY = OFF,ONLINE = OFF) ON [PRIMARY]
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
SET ANSI_PADDING ON
CREATE TABLE [dbo].[x_gds_shared_resource](
        [id]                   [bigint] IDENTITY(1,1) NOT NULL,
        [guid]                 [varchar](64) NOT NULL,
        [create_time]          [datetime2] DEFAULT NULL NULL,
        [update_time]          [datetime2] DEFAULT NULL NULL,
        [added_by_id]          [bigint] DEFAULT NULL NULL,
        [upd_by_id]            [bigint] DEFAULT NULL NULL,
        [version]              [bigint] NOT NULL DEFAULT 1,
        [is_enabled]           [tinyint] DEFAULT 1 NOT NULL,
        [name]                 [varchar](512) NOT NULL,
        [description]          [nvarchar](max) DEFAULT NULL  NULL,
        [data_share_id]        [bigint] NOT NULL,
        [resource]             [nvarchar](max) NOT NULL,
        [resource_signature]   [varchar](128) NOT NULL,
        [sub_resource]         [nvarchar](max) DEFAULT NULL NULL,
        [sub_resource_type]    [nvarchar](max) DEFAULT NULL NULL,
        [condition_expr]       [nvarchar](max) DEFAULT NULL NULL,
        [access_types]         [nvarchar](max) DEFAULT NULL NULL,
        [row_filter]           [nvarchar](max) DEFAULT NULL NULL,
        [sub_resource_masks]   [nvarchar](max) DEFAULT NULL NULL,
        [profiles]             [nvarchar](max) DEFAULT NULL NULL,
        [options]              [nvarchar](max) DEFAULT NULL NULL,
        [additional_info]      [nvarchar](max) DEFAULT NULL NULL,
PRIMARY KEY CLUSTERED
(
        [id] ASC
)WITH (PAD_INDEX = OFF,STATISTICS_NORECOMPUTE = OFF,IGNORE_DUP_KEY = OFF,ALLOW_ROW_LOCKS = ON,ALLOW_PAGE_LOCKS = ON) ON [PRIMARY],
 CONSTRAINT [x_gds_shared_resource$x_gds_shared_resource_UK_name] UNIQUE NONCLUSTERED
(
        [data_share_id] ASC, [name] ASC
)WITH (PAD_INDEX = OFF,STATISTICS_NORECOMPUTE = OFF,IGNORE_DUP_KEY = OFF,ALLOW_ROW_LOCKS = ON,ALLOW_PAGE_LOCKS = ON) ON [PRIMARY],
CONSTRAINT [x_gds_shared_resource$x_gds_shared_resource_UK_resource_signature] UNIQUE NONCLUSTERED
(
        [data_share_id] ASC, [resource_signature] ASC
)WITH (PAD_INDEX = OFF,STATISTICS_NORECOMPUTE = OFF,IGNORE_DUP_KEY = OFF,ALLOW_ROW_LOCKS = ON,ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

) ON [PRIMARY]
GO
ALTER TABLE [dbo].[x_gds_shared_resource]  WITH CHECK ADD  CONSTRAINT [x_gds_shared_resource_FK_added_by_id] FOREIGN KEY([added_by_id]) REFERENCES [dbo].[x_portal_user] ([id])
ALTER TABLE [dbo].[x_gds_shared_resource]  WITH CHECK ADD  CONSTRAINT [x_gds_shared_resource_FK_upd_by_id] FOREIGN KEY([upd_by_id]) REFERENCES [dbo].[x_portal_user] ([id])
ALTER TABLE [dbo].[x_gds_shared_resource]  WITH CHECK ADD  CONSTRAINT [x_gds_shared_resource_FK_data_share_id] FOREIGN KEY([data_share_id]) REFERENCES [dbo].[x_gds_data_share] ([id])
GO
CREATE NONCLUSTERED INDEX [x_gds_shared_resource_guid] ON [x_gds_shared_resource]
(
   [guid] ASC
)
WITH (SORT_IN_TEMPDB = OFF,DROP_EXISTING = OFF,IGNORE_DUP_KEY = OFF,ONLINE = OFF) ON [PRIMARY]
CREATE NONCLUSTERED INDEX [x_gds_shared_resource_data_share_id] ON [x_gds_shared_resource]
(
   [data_share_id] ASC
)
WITH (SORT_IN_TEMPDB = OFF,DROP_EXISTING = OFF,IGNORE_DUP_KEY = OFF,ONLINE = OFF) ON [PRIMARY]
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
SET ANSI_PADDING ON
CREATE TABLE [dbo].[x_gds_data_share_in_dataset](
        [id]                   [bigint] IDENTITY(1,1) NOT NULL,
        [guid]                 [varchar](64) NOT NULL,
        [create_time]          [datetime2] DEFAULT NULL NULL,
        [update_time]          [datetime2] DEFAULT NULL NULL,
        [added_by_id]          [bigint] DEFAULT NULL NULL,
        [upd_by_id]            [bigint] DEFAULT NULL NULL,
        [version]              [bigint] NOT NULL DEFAULT 1,
        [is_enabled]           [tinyint] DEFAULT 1 NOT NULL,
        [description]          [nvarchar](max) DEFAULT NULL  NULL,
        [data_share_id]        [bigint] NOT NULL,
        [dataset_id]           [bigint] NOT NULL,
        [status]               [smallint] NOT NULL,
        [validity_period]      [nvarchar](max) DEFAULT NULL NULL,
        [profiles]             [nvarchar](max) DEFAULT NULL NULL,
        [options]              [nvarchar](max) DEFAULT NULL NULL,
        [additional_info]      [nvarchar](max) DEFAULT NULL NULL,
        [approver_id]          [bigint] DEFAULT NULL NULL,
PRIMARY KEY CLUSTERED
(
        [id] ASC
)WITH (PAD_INDEX = OFF,STATISTICS_NORECOMPUTE = OFF,IGNORE_DUP_KEY = OFF,ALLOW_ROW_LOCKS = ON,ALLOW_PAGE_LOCKS = ON) ON [PRIMARY],
CONSTRAINT [x_gds_data_share_in_dataset$x_gds_dshid_UK_data_share_id_dataset_id] UNIQUE NONCLUSTERED
(
        [data_share_id] ASC, [dataset_id] ASC
)WITH (PAD_INDEX = OFF,STATISTICS_NORECOMPUTE = OFF,IGNORE_DUP_KEY = OFF,ALLOW_ROW_LOCKS = ON,ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
ALTER TABLE [dbo].[x_gds_data_share_in_dataset]  WITH CHECK ADD  CONSTRAINT [x_gds_dshid_FK_added_by_id] FOREIGN KEY([added_by_id]) REFERENCES [dbo].[x_portal_user] ([id])
ALTER TABLE [dbo].[x_gds_data_share_in_dataset]  WITH CHECK ADD  CONSTRAINT [x_gds_dshid_FK_upd_by_id] FOREIGN KEY([upd_by_id]) REFERENCES [dbo].[x_portal_user] ([id])
ALTER TABLE [dbo].[x_gds_data_share_in_dataset]  WITH CHECK ADD  CONSTRAINT [x_gds_dshid_FK_data_share_id] FOREIGN KEY([data_share_id]) REFERENCES [dbo].[x_gds_data_share] ([id])
ALTER TABLE [dbo].[x_gds_data_share_in_dataset]  WITH CHECK ADD  CONSTRAINT [x_gds_dshid_FK_dataset_id] FOREIGN KEY([dataset_id]) REFERENCES [dbo].[x_gds_dataset] ([id])
ALTER TABLE [dbo].[x_gds_data_share_in_dataset]  WITH CHECK ADD  CONSTRAINT [x_gds_dshid_FK_approver_id] FOREIGN KEY([approver_id]) REFERENCES [dbo].[x_portal_user] ([id])
GO
CREATE NONCLUSTERED INDEX [x_gds_dshid_guid] ON [x_gds_data_share_in_dataset]
(
   [guid] ASC
)
WITH (SORT_IN_TEMPDB = OFF,DROP_EXISTING = OFF,IGNORE_DUP_KEY = OFF,ONLINE = OFF) ON [PRIMARY]
CREATE NONCLUSTERED INDEX [x_gds_dshid_data_share_id] ON [x_gds_data_share_in_dataset]
(
   [data_share_id] ASC
)
WITH (SORT_IN_TEMPDB = OFF,DROP_EXISTING = OFF,IGNORE_DUP_KEY = OFF,ONLINE = OFF) ON [PRIMARY]
CREATE NONCLUSTERED INDEX [x_gds_dshid_dataset_id] ON [x_gds_data_share_in_dataset]
(
   [dataset_id] ASC
)
WITH (SORT_IN_TEMPDB = OFF,DROP_EXISTING = OFF,IGNORE_DUP_KEY = OFF,ONLINE = OFF) ON [PRIMARY]
CREATE NONCLUSTERED INDEX [x_gds_dshid_data_share_id_dataset_id] ON [x_gds_data_share_in_dataset]
(
   [data_share_id] ASC, [dataset_id] ASC
)
WITH (SORT_IN_TEMPDB = OFF,DROP_EXISTING = OFF,IGNORE_DUP_KEY = OFF,ONLINE = OFF) ON [PRIMARY]
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
SET ANSI_PADDING ON
CREATE TABLE [dbo].[x_gds_dataset_in_project](
        [id]                   [bigint] IDENTITY(1,1) NOT NULL,
        [guid]                 [varchar](64) NOT NULL,
        [create_time]          [datetime2] DEFAULT NULL NULL,
        [update_time]          [datetime2] DEFAULT NULL NULL,
        [added_by_id]          [bigint] DEFAULT NULL NULL,
        [upd_by_id]            [bigint] DEFAULT NULL NULL,
        [version]              [bigint] NOT NULL DEFAULT 1,
        [is_enabled]           [tinyint] DEFAULT 1 NOT NULL,
        [description]          [nvarchar](max) DEFAULT NULL  NULL,
        [dataset_id]           [bigint] NOT NULL,
        [project_id]           [bigint] NOT NULL,
        [status]               [smallint] NOT NULL,
        [validity_period]      [nvarchar](max) DEFAULT NULL NULL,
        [profiles]             [nvarchar](max) DEFAULT NULL NULL,
        [options]              [nvarchar](max) DEFAULT NULL NULL,
        [additional_info]      [nvarchar](max) DEFAULT NULL NULL,
        [approver_id]          [bigint] DEFAULT NULL NULL,
PRIMARY KEY CLUSTERED
(
        [id] ASC
)WITH (PAD_INDEX = OFF,STATISTICS_NORECOMPUTE = OFF,IGNORE_DUP_KEY = OFF,ALLOW_ROW_LOCKS = ON,ALLOW_PAGE_LOCKS = ON) ON [PRIMARY],
CONSTRAINT [x_gds_dataset_in_project$x_gds_dip_UK_data_share_id_dataset_id] UNIQUE NONCLUSTERED
(
        [dataset_id] ASC, [project_id] ASC
)WITH (PAD_INDEX = OFF,STATISTICS_NORECOMPUTE = OFF,IGNORE_DUP_KEY = OFF,ALLOW_ROW_LOCKS = ON,ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
ALTER TABLE [dbo].[x_gds_dataset_in_project]  WITH CHECK ADD  CONSTRAINT [x_gds_dip_FK_added_by_id] FOREIGN KEY([added_by_id]) REFERENCES [dbo].[x_portal_user] ([id])
ALTER TABLE [dbo].[x_gds_dataset_in_project]  WITH CHECK ADD  CONSTRAINT [x_gds_dip_FK_upd_by_id] FOREIGN KEY([upd_by_id]) REFERENCES [dbo].[x_portal_user] ([id])
ALTER TABLE [dbo].[x_gds_dataset_in_project]  WITH CHECK ADD  CONSTRAINT [x_gds_dip_FK_dataset_id] FOREIGN KEY([dataset_id]) REFERENCES [dbo].[x_gds_dataset] ([id])
ALTER TABLE [dbo].[x_gds_dataset_in_project]  WITH CHECK ADD  CONSTRAINT [x_gds_dip_FK_project_id] FOREIGN KEY([project_id]) REFERENCES [dbo].[x_gds_project] ([id])
ALTER TABLE [dbo].[x_gds_dataset_in_project]  WITH CHECK ADD  CONSTRAINT [x_gds_dip_FK_approver_id] FOREIGN KEY([approver_id]) REFERENCES [dbo].[x_portal_user] ([id])
GO
CREATE NONCLUSTERED INDEX [x_gds_dip_guid] ON [x_gds_dataset_in_project]
(
   [guid] ASC
)
WITH (SORT_IN_TEMPDB = OFF,DROP_EXISTING = OFF,IGNORE_DUP_KEY = OFF,ONLINE = OFF) ON [PRIMARY]
CREATE NONCLUSTERED INDEX [x_gds_dip_dataset_id] ON [x_gds_dataset_in_project]
(
   [dataset_id] ASC
)
WITH (SORT_IN_TEMPDB = OFF,DROP_EXISTING = OFF,IGNORE_DUP_KEY = OFF,ONLINE = OFF) ON [PRIMARY]
CREATE NONCLUSTERED INDEX [x_gds_dip_project_id] ON [x_gds_dataset_in_project]
(
   [project_id] ASC
)
WITH (SORT_IN_TEMPDB = OFF,DROP_EXISTING = OFF,IGNORE_DUP_KEY = OFF,ONLINE = OFF) ON [PRIMARY]
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
SET ANSI_PADDING ON
CREATE TABLE [dbo].[x_gds_dataset_policy_map](
        [id]                   [bigint] IDENTITY(1,1) NOT NULL,
        [dataset_id]           [bigint] NOT NULL,
        [policy_id]            [bigint] NOT NULL,
PRIMARY KEY CLUSTERED
(
        [id] ASC
)WITH (PAD_INDEX = OFF,STATISTICS_NORECOMPUTE = OFF,IGNORE_DUP_KEY = OFF,ALLOW_ROW_LOCKS = ON,ALLOW_PAGE_LOCKS = ON) ON [PRIMARY],
CONSTRAINT [x_gds_dataset_policy_map$x_gds_dpm_UK_dataset_id_policy_id] UNIQUE NONCLUSTERED
(
        [dataset_id] ASC, [policy_id] ASC
)WITH (PAD_INDEX = OFF,STATISTICS_NORECOMPUTE = OFF,IGNORE_DUP_KEY = OFF,ALLOW_ROW_LOCKS = ON,ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
ALTER TABLE [dbo].[x_gds_dataset_policy_map]  WITH CHECK ADD  CONSTRAINT [x_gds_dpm_FK_dataset_id] FOREIGN KEY([dataset_id]) REFERENCES [dbo].[x_gds_dataset] ([id])
ALTER TABLE [dbo].[x_gds_dataset_policy_map]  WITH CHECK ADD  CONSTRAINT [x_gds_dpm_FK_policy_id] FOREIGN KEY([policy_id]) REFERENCES [dbo].[x_policy] ([id])
GO
CREATE NONCLUSTERED INDEX [x_gds_dpm_dataset_id] ON [x_gds_dataset_policy_map]
(
   [dataset_id] ASC
)
WITH (SORT_IN_TEMPDB = OFF,DROP_EXISTING = OFF,IGNORE_DUP_KEY = OFF,ONLINE = OFF) ON [PRIMARY]
CREATE NONCLUSTERED INDEX [x_gds_dpm_policy_id] ON [x_gds_dataset_policy_map]
(
   [policy_id] ASC
)
WITH (SORT_IN_TEMPDB = OFF,DROP_EXISTING = OFF,IGNORE_DUP_KEY = OFF,ONLINE = OFF) ON [PRIMARY]
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
SET ANSI_PADDING ON
CREATE TABLE [dbo].[x_gds_project_policy_map](
        [id]                   [bigint] IDENTITY(1,1) NOT NULL,
        [project_id]           [bigint] NOT NULL,
        [policy_id]            [bigint] NOT NULL,
PRIMARY KEY CLUSTERED
(
        [id] ASC
)WITH (PAD_INDEX = OFF,STATISTICS_NORECOMPUTE = OFF,IGNORE_DUP_KEY = OFF,ALLOW_ROW_LOCKS = ON,ALLOW_PAGE_LOCKS = ON) ON [PRIMARY],
CONSTRAINT [x_gds_project_policy_map$x_gds_ppm_UK_project_id_policy_id] UNIQUE NONCLUSTERED
(
        [project_id] ASC, [policy_id] ASC
)WITH (PAD_INDEX = OFF,STATISTICS_NORECOMPUTE = OFF,IGNORE_DUP_KEY = OFF,ALLOW_ROW_LOCKS = ON,ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
ALTER TABLE [dbo].[x_gds_project_policy_map]  WITH CHECK ADD  CONSTRAINT [x_gds_ppm_FK_project_id] FOREIGN KEY([project_id]) REFERENCES [dbo].[x_gds_project] ([id])
ALTER TABLE [dbo].[x_gds_project_policy_map]  WITH CHECK ADD  CONSTRAINT [x_gds_ppm_FK_policy_id] FOREIGN KEY([policy_id]) REFERENCES [dbo].[x_policy] ([id])
GO
CREATE NONCLUSTERED INDEX [x_gds_ppm_project_id] ON [x_gds_project_policy_map]
(
   [project_id] ASC
)
WITH (SORT_IN_TEMPDB = OFF,DROP_EXISTING = OFF,IGNORE_DUP_KEY = OFF,ONLINE = OFF) ON [PRIMARY]
CREATE NONCLUSTERED INDEX [x_gds_ppm_policy_id] ON [x_gds_project_policy_map]
(
   [policy_id] ASC
)
WITH (SORT_IN_TEMPDB = OFF,DROP_EXISTING = OFF,IGNORE_DUP_KEY = OFF,ONLINE = OFF) ON [PRIMARY]
GO

exit
