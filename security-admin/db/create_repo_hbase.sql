# Replace the following:
#  %REPOSITORY_NAME%
#  %REPOSITORY_DESC%
#  %USERNAME%
#  %PASSWORD%
#  %FS_DEFAULT_NAME%
#  %HADOOP_SECURITY_AUTHORIZATION%
#  %HADOOP_SECURITY_AUTHENTICATION%
#  %HADOOP_SECURITY_AUTH_TO_LOCAL%
#  %DFS_DATANODE_KERBEROS_PRINCIPAL%
#  %DFS_NAMENODE_KERBEROS_PRINCIPAL%
#  %DFS_SECONDARY_NAMENODE_KERBEROS_PRINCIPAL%
#  %HBASE_MASTER_KERBEROS_PRINCIPAL%
#  %HBASE_RPC_ENGINE%
#  %HBASE_RPC_PROTECTION%
#  %HBASE_SECURITY_AUTHENTICATION%
#  %HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT%
#  %HBASE_ZOOKEEPER_QUORUM%
#  %ZOOKEEPER_ZNODE_PARENT%
#  %COMMON_NAME_FOR_CERTIFICATE%
#
# Example:
#  INSERT INTO x_asset (asset_name, descr, act_status, asset_type, config, create_time, update_time, added_by_id, upd_by_id)
#   VALUES ('hbasetest', 'hbase test repository', 1, 2, '{\"username\":\"policymgr\",\"password\":\"policymgr\",\"fs.default.name\":\"hdfs://sandbox.hortonworks.com:8020\",\"hadoop.security.authorization\":\"true\",\"hadoop.security.authentication\":\"simple\",\"hadoop.security.auth_to_local\":\"\",\"dfs.datanode.kerberos.principal\":\"\",\"dfs.namenode.kerberos.principal\":\"\",\"dfs.secondary.namenode.kerberos.principal\":\"\",\"hbase.master.kerberos.principal\":\"\",\"hbase.rpc.engine\":\"org.apache.hadoop.hbase.ipc.SecureRpcEngine%\",\"hbase.rpc.protection\":\"PRIVACY\",\"hbase.security.authentication\":\"simple\",\"hbase.zookeeper.property.clientPort\":\"2181\",\"hbase.zookeeper.quorum\":\"sandbox.hortonworks.com\",\"zookeeper.znode.parent\":\"/hbase-unsecure\",\"commonNameForCertificate\":\"\"}', now(), now(), 1, 1);
#

# create the repository
INSERT INTO x_asset (asset_name, descr, act_status, asset_type, config, create_time, update_time, added_by_id, upd_by_id)
 VALUES ('%REPOSITORY_NAME%', '%REPOSITORY_DESC%', 1, 2, '{\"username\":\"%USERNAME%\",\"password\":\"%PASSWORD%\",\"fs.default.name\":\"%FS_DEFAULT_NAME%\",\"hadoop.security.authorization\":\"%HADOOP_SECURITY_AUTHORIZATION%\",\"hadoop.security.authentication\":\"%HADOOP_SECURITY_AUTHENTICATION%\",\"hadoop.security.auth_to_local\":\"%HADOOP_SECURITY_AUTH_TO_LOCAL%\",\"dfs.datanode.kerberos.principal\":\"%DFS_DATANODE_KERBEROS_PRINCIPAL%\",\"dfs.namenode.kerberos.principal\":\"%DFS_NAMENODE_KERBEROS_PRINCIPAL%\",\"dfs.secondary.namenode.kerberos.principal\":\"%DFS_SECONDARY_NAMENODE_KERBEROS_PRINCIPAL%\",\"hbase.master.kerberos.principal\":\"%HBASE_MASTER_KERBEROS_PRINCIPAL%\",\"hbase.rpc.engine\":\"%HBASE_RPC_ENGINE%\",\"hbase.rpc.protection\":\"%HBASE_RPC_PROTECTION%\",\"hbase.security.authentication\":\"%HBASE_SECURITY_AUTHENTICATION%\",\"hbase.zookeeper.property.clientPort\":\"%HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT%\",\"hbase.zookeeper.quorum\":\"%HBASE_ZOOKEEPER_QUORUM%\",\"zookeeper.znode.parent\":\"%ZOOKEEPER_ZNODE_PARENT%\",\"commonNameForCertificate\":\"%COMMON_NAME_FOR_CERTIFICATE%\"}', now(), now(), 1, 1);
SELECT @asset_id := id FROM x_asset WHERE asset_name='%REPOSITORY_NAME%' and act_status = 1;

# create default policy to allow access to public
INSERT INTO x_resource (policy_name, res_name, descr, res_type, asset_id, is_encrypt, is_recursive, res_tables, res_col_fams, res_cols, res_status, table_type, col_type, create_time, update_time, added_by_id, upd_by_id) 
 VALUES ('default-hbase', '/*/*/*', 'Default policy', 1, @asset_id, 2, 0, '*', '*', '*', 1, 0, 0, now(), now(), 1, 1);
SELECT @resource_id := id FROM x_resource WHERE policy_name='default-hbase';

DELIMITER //
DROP PROCEDURE IF EXISTS CreateXAGroup;
CREATE PROCEDURE CreateXAGroup(in groupName varchar(1024))
BEGIN
  DECLARE groupId bigint(20);

  SELECT g.id INTO groupId FROM x_group g WHERE g.group_name = groupName;

  IF groupId IS NULL THEN
	SELECT CONCAT('Creating group ', groupName);
    INSERT INTO x_group (group_name, descr, status, group_type, create_time, update_time, added_by_id, upd_by_id) VALUES (groupName, groupName, 0, 1, now(), now(), 1, 1);
  ELSE
    SELECT CONCAT('Group ', groupName, ' already exists');
  END IF;
END //
DELIMITER ;
CALL CreateXAGroup('public');
DROP PROCEDURE IF EXISTS CreateXAGroup;

SELECT @group_public := id FROM x_group WHERE group_name='public';

SELECT @perm_read   := 2;
SELECT @perm_write  := 3;
SELECT @perm_create := 4;
SELECT @perm_admin  := 6;

INSERT INTO x_perm_map (res_id, group_id, perm_for, perm_type, perm_group, is_recursive, is_wild_card, grant_revoke, create_time, update_time, added_by_id, upd_by_id) VALUES (@resource_id, @group_public, 2, @perm_read, now(), 0, 1, 1, now(), now(), 1, 1);

INSERT INTO x_perm_map (res_id, group_id, perm_for, perm_type, perm_group, is_recursive, is_wild_card, grant_revoke, create_time, update_time, added_by_id, upd_by_id) VALUES (@resource_id, @group_public, 2, @perm_write, now(), 0, 1, 1, now(), now(), 1, 1);

INSERT INTO x_perm_map (res_id, group_id, perm_for, perm_type, perm_group, is_recursive, is_wild_card, grant_revoke, create_time, update_time, added_by_id, upd_by_id) VALUES (@resource_id, @group_public, 2, @perm_create, now(), 0, 1, 1, now(), now(), 1, 1);

INSERT INTO x_perm_map (res_id, group_id, perm_for, perm_type, perm_group, is_recursive, is_wild_card, grant_revoke, create_time, update_time, added_by_id, upd_by_id) VALUES (@resource_id, @group_public, 2, @perm_admin, now(), 0, 1, 1, now(), now(), 1, 1);

# Enable auditing
INSERT INTO x_audit_map (res_id, audit_type, create_time, update_time, added_by_id, upd_by_id) VALUES (@resource_id, 1, now(), now(), 1, 1);
