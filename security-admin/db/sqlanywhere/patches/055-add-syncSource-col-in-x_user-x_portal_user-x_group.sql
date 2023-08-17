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

IF NOT EXISTS(select * from SYS.SYSCOLUMNS where tname = 'x_portal_user' and cname = 'sync_source') THEN
		ALTER TABLE dbo.x_portal_user ADD sync_source text DEFAULT NULL NULL;
END IF;
GO
IF NOT EXISTS(select * from SYS.SYSCOLUMNS where tname = 'x_user' and cname = 'sync_source') THEN
		ALTER TABLE dbo.x_user ADD sync_source text DEFAULT NULL NULL;
END IF;
GO
IF NOT EXISTS(select * from SYS.SYSCOLUMNS where tname = 'x_group' and cname = 'sync_source') THEN
		ALTER TABLE dbo.x_group ADD sync_source text DEFAULT NULL NULL;
END IF;
GO
exit