package com.xasecure.authorization.hive.authorizer;

import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizer;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveMetastoreClientFactory;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrincipal;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilege;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeInfo;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveRoleGrant;
import org.apache.hadoop.security.UserGroupInformation;

public class XaSecureHiveAuthorizerBase implements HiveAuthorizer {

	private HiveMetastoreClientFactory mMetastoreClientFactory;
	private HiveConf                   mHiveConf;
	private HiveAuthenticationProvider mHiveAuthenticator;
	private UserGroupInformation       mUgi;
	  
	public XaSecureHiveAuthorizerBase(HiveMetastoreClientFactory metastoreClientFactory,
									  HiveConf                   hiveConf,
									  HiveAuthenticationProvider hiveAuthenticator) {
		mMetastoreClientFactory = metastoreClientFactory;
		mHiveConf               = hiveConf;
		mHiveAuthenticator      = hiveAuthenticator;

		String userName = mHiveAuthenticator == null ? null : mHiveAuthenticator.getUserName();

		mUgi = userName == null ? null : UserGroupInformation.createRemoteUser(userName);
	}

	public HiveMetastoreClientFactory getMetastoreClientFactory() {
		return mMetastoreClientFactory;
	}

	public HiveConf getHiveConf() {
		return mHiveConf;
	}

	public HiveAuthenticationProvider getHiveAuthenticator() {
		return mHiveAuthenticator;
	}

	public UserGroupInformation getCurrentUserGroupInfo() {
		return mUgi;
	}

	@Override
	public void applyAuthorizationConfigPolicy(HiveConf arg0) {
		// TODO Auto-generated method stub
	}

	@Override
	public void checkPrivileges(HiveOperationType         hiveOpType,
								List<HivePrivilegeObject> inputsHObjs,
								List<HivePrivilegeObject> outputHObjs,
								HiveAuthzContext          context)
										throws HiveAuthzPluginException, HiveAccessControlException {
		// TODO Auto-generated method stub
	}

	@Override
	public void createRole(String arg0, HivePrincipal arg1)
			throws HiveAuthzPluginException, HiveAccessControlException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void dropRole(String arg0) throws HiveAuthzPluginException,
			HiveAccessControlException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public List<String> getAllRoles() throws HiveAuthzPluginException,
			HiveAccessControlException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> getCurrentRoleNames() throws HiveAuthzPluginException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<HiveRoleGrant> getPrincipalGrantInfoForRole(String arg0)
			throws HiveAuthzPluginException, HiveAccessControlException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<HiveRoleGrant> getRoleGrantInfoForPrincipal(HivePrincipal arg0)
			throws HiveAuthzPluginException, HiveAccessControlException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public VERSION getVersion() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void grantPrivileges(List<HivePrincipal> arg0,
			List<HivePrivilege> arg1, HivePrivilegeObject arg2,
			HivePrincipal arg3, boolean arg4) throws HiveAuthzPluginException,
			HiveAccessControlException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void grantRole(List<HivePrincipal> arg0, List<String> arg1,
			boolean arg2, HivePrincipal arg3) throws HiveAuthzPluginException,
			HiveAccessControlException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void revokePrivileges(List<HivePrincipal> arg0,
			List<HivePrivilege> arg1, HivePrivilegeObject arg2,
			HivePrincipal arg3, boolean arg4) throws HiveAuthzPluginException,
			HiveAccessControlException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void revokeRole(List<HivePrincipal> arg0, List<String> arg1,
			boolean arg2, HivePrincipal arg3) throws HiveAuthzPluginException,
			HiveAccessControlException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setCurrentRole(String arg0) throws HiveAccessControlException,
			HiveAuthzPluginException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public List<HivePrivilegeInfo> showPrivileges(HivePrincipal arg0,
			HivePrivilegeObject arg1) throws HiveAuthzPluginException,
			HiveAccessControlException {
		// TODO Auto-generated method stub
		return null;
	}

}
