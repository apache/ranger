SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[ranger_masterkey](
	[id] [bigint] IDENTITY(1,1) NOT NULL,
	[create_time] [datetime2] DEFAULT NULL NULL,
	[update_time] [datetime2] DEFAULT NULL NULL,
	[added_by_id] [bigint] DEFAULT NULL NULL,
	[upd_by_id] [bigint] DEFAULT NULL NULL,
	[cipher] [varchar](255) DEFAULT NULL NULL,
	[bitlength] [int] DEFAULT NULL NULL,
	[masterkey] [varchar](2048),
PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF,STATISTICS_NORECOMPUTE = OFF,IGNORE_DUP_KEY = OFF,ALLOW_ROW_LOCKS = ON,ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[ranger_keystore](
	[id] [bigint] IDENTITY(1,1) NOT NULL,
	[create_time] [datetime2] DEFAULT NULL NULL,
	[update_time] [datetime2] DEFAULT NULL NULL,
	[added_by_id] [bigint] DEFAULT NULL NULL,
	[kms_alias] [varchar](255) NOT NULL,
	[kms_createdDate] [bigint] DEFAULT NULL NULL,
	[kms_cipher] [varchar](255) DEFAULT NULL NULL,
	[kms_bitLength] [bigint] DEFAULT NULL NULL,
	[kms_description] [varchar](512) DEFAULT NULL NULL,
	[kms_version] [bigint] DEFAULT NULL NULL,
	[kms_attributes] [varchar](1024) DEFAULT NULL NULL,
	[kms_encoded] [varchar](2048),
PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF,STATISTICS_NORECOMPUTE = OFF,IGNORE_DUP_KEY = OFF,ALLOW_ROW_LOCKS = ON,ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO