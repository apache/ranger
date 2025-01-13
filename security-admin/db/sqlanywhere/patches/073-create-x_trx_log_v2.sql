-- Licensed to the Apache Software Foundation(ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
--(the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--	 http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

call dbo.removeForeignKeysAndTable('x_trx_log_v2')
GO

create table dbo.x_trx_log_v2(
	id bigint IDENTITY NOT NULL,
	create_time datetime DEFAULT NULL NULL,
	added_by_id bigint DEFAULT NULL NULL,
	class_type int DEFAULT 0 NOT NULL,
	object_id bigint DEFAULT NULL NULL,
	parent_object_id bigint DEFAULT NULL NULL,
	parent_object_class_type int DEFAULT 0 NOT NULL,
	parent_object_name varchar(1024) DEFAULT NULL NULL,
	object_name varchar(1024) DEFAULT NULL NULL,
	change_info text DEFAULT NULL NULL,
	trx_id varchar(1024)DEFAULT NULL NULL,
	action varchar(255) DEFAULT NULL NULL,
	sess_id varchar(512) DEFAULT NULL NULL,
	req_id varchar(30) DEFAULT NULL NULL,
	sess_type varchar(30) DEFAULT NULL NULL,
	CONSTRAINT x_trx_log_v2_PK_id PRIMARY KEY CLUSTERED(id)
)
GO

CREATE NONCLUSTERED INDEX x_trx_log_v2_FK_cr_time ON dbo.x_trx_log_v2(create_time ASC)
GO

CREATE NONCLUSTERED INDEX x_trx_log_v2_FK_added_by_id ON dbo.x_trx_log_v2(added_by_id ASC)
GO

CREATE NONCLUSTERED INDEX x_trx_log_v2_FK_trx_id ON dbo.x_trx_log_v2(trx_id ASC)
GO
EXIT