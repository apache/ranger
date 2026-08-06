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
IF (OBJECT_ID('x_audit_metrics') IS NOT NULL)
BEGIN
    DROP TABLE [dbo].[x_audit_metrics]
END

GO
SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
SET ANSI_PADDING ON
CREATE TABLE [dbo].[x_audit_metrics](
[id] [bigint] IDENTITY(1,1) NOT NULL,
[service_type] [bigint] DEFAULT NULL NULL,
[service_name] [nvarchar](255) DEFAULT NULL NULL,
[app_id] [nvarchar](255) DEFAULT NULL NULL,
[cluster_name] [nvarchar](255) DEFAULT NULL NULL,
[client_ip] [nvarchar](255) DEFAULT NULL NULL,
[metrics_text] [nvarchar](4000) DEFAULT NULL NULL,
[throughput_unit] [nvarchar](255) DEFAULT NULL NULL,
[number_of_audits] bigint DEFAULT NULL NULL,
[version] [bigint] DEFAULT NULL NULL,
[create_time] [datetime2] DEFAULT NULL NULL,
[update_time] [datetime2] DEFAULT NULL NULL,
[added_by_id] [bigint] DEFAULT NULL NULL,
[upd_by_id] [bigint] DEFAULT NULL NULL,
  PRIMARY KEY CLUSTERED
(
        [id] ASC
)WITH (PAD_INDEX = OFF,STATISTICS_NORECOMPUTE = OFF,IGNORE_DUP_KEY = OFF,ALLOW_ROW_LOCKS = ON,ALLOW_PAGE_LOCKS = ON) ON [PRIMARY])
GO
ALTER TABLE [dbo].[x_audit_metrics] WITH CHECK ADD CONSTRAINT [x_audit_metrics_FK_service_type] FOREIGN KEY([service_type]) REFERENCES [dbo].[x_service_def] ([id])
GO
ALTER TABLE [dbo].[x_audit_metrics] WITH CHECK ADD CONSTRAINT [x_audit_metrics_FK_added_by_id] FOREIGN KEY([added_by_id]) REFERENCES [dbo].[x_portal_user] ([id])
GO
ALTER TABLE [dbo].[x_audit_metrics] WITH CHECK ADD CONSTRAINT [x_audit_metrics_FK_upd_by_id] FOREIGN KEY([upd_by_id]) REFERENCES [dbo].[x_portal_user] ([id])
GO
exit