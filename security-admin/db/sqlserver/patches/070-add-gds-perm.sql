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

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF EXISTS (SELECT *
           FROM   sys.objects
           WHERE  object_id = OBJECT_ID(N'dbo.getXportalUIdByLoginId')
                  AND type IN ( N'FN', N'IF', N'TF', N'FS', N'FT' ))
  DROP FUNCTION dbo.getXportalUIdByLoginId
  PRINT 'Dropped function dbo.getXportalUIdByLoginId'

GO
PRINT 'Creating function dbo.getXportalUIdByLoginId'
GO
CREATE FUNCTION dbo.getXportalUIdByLoginId
(

        @inputValue varchar(200)
)
RETURNS int
AS
BEGIN
        Declare @myid int;

        Select @myid = id from x_portal_user where x_portal_user.login_id = @inputValue;

        return @myid;

END
GO

PRINT 'Creating function dbo.getXportalUIdByLoginId'
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF EXISTS (SELECT *
           FROM   sys.objects
           WHERE  object_id = OBJECT_ID(N'dbo.getModulesIdByName')
                  AND type IN ( N'FN', N'IF', N'TF', N'FS', N'FT' ))
  DROP FUNCTION dbo.getModulesIdByName
  PRINT 'Dropped function dbo.getModulesIdByName'

GO
PRINT 'Creating function dbo.getModulesIdByName'
GO
CREATE FUNCTION dbo.getModulesIdByName
(

        @inputValue varchar(200)
)
RETURNS int
AS
BEGIN
        Declare @myid int;

        Select @myid = id from x_modules_master where module = @inputValue;

        return @myid;

END
GO

PRINT 'Created function dbo.getModulesIdByName successfully'
GO

IF NOT EXISTS(select * from x_modules_master where module = 'Governed Data Sharing')
BEGIN
        INSERT INTO x_modules_master(create_time,update_time,added_by_id,upd_by_id,module,url) VALUES(CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,dbo.getXportalUIdByLoginId('admin'),dbo.getXportalUIdByLoginId('admin'),'Governed Data Sharing','');
END
GO
IF NOT EXISTS(select * from x_user_module_perm where user_id=dbo.getXportalUIdByLoginId('admin') and module_id=dbo.getModulesIdByName('Governed Data Sharing'))
BEGIN
        INSERT INTO x_user_module_perm (user_id,module_id,create_time,update_time,added_by_id,upd_by_id,is_allowed) VALUES (dbo.getXportalUIdByLoginId('admin'),dbo.getModulesIdByName('Governed Data Sharing'),CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,dbo.getXportalUIdByLoginId('admin'),dbo.getXportalUIdByLoginId('admin'),1);
END
GO
IF NOT EXISTS(select * from x_user_module_perm where user_id=dbo.getXportalUIdByLoginId('rangerusersync') and module_id=dbo.getModulesIdByName('Governed Data Sharing'))
BEGIN
        INSERT INTO x_user_module_perm (user_id,module_id,create_time,update_time,added_by_id,upd_by_id,is_allowed) VALUES (dbo.getXportalUIdByLoginId('rangerusersync'),dbo.getModulesIdByName('Governed Data Sharing'),CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,dbo.getXportalUIdByLoginId('admin'),dbo.getXportalUIdByLoginId('admin'),1);
END
GO
IF NOT EXISTS(select * from x_user_module_perm where user_id=dbo.getXportalUIdByLoginId('rangertagsync') and module_id=dbo.getModulesIdByName('Governed Data Sharing'))
BEGIN
        INSERT INTO x_user_module_perm (user_id,module_id,create_time,update_time,added_by_id,upd_by_id,is_allowed) VALUES (dbo.getXportalUIdByLoginId('rangertagsync'),dbo.getModulesIdByName('Governed Data Sharing'),CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,dbo.getXportalUIdByLoginId('admin'),dbo.getXportalUIdByLoginId('admin'),1);
END
GO
exit
