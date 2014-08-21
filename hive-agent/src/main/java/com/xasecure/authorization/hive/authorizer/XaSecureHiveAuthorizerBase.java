package com.xasecure.authorization.hive.authorizer;

import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizer;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveMetastoreClientFactory;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrincipal;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilege;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeInfo;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveRoleGrant;
import org.apache.hadoop.security.UserGroupInformation;

import com.xasecure.authorization.hive.XaHiveAccessContext;

public class XaSecureHiveAuthorizerBase implements HiveAuthorizer {

	private HiveMetastoreClientFactory mMetastoreClientFactory;
	private HiveConf                   mHiveConf;
	private HiveAuthenticationProvider mHiveAuthenticator;
	private HiveAuthzSessionContext    mSessionContext;
	private UserGroupInformation       mUgi;
	  
	public XaSecureHiveAuthorizerBase(HiveMetastoreClientFactory metastoreClientFactory,
									  HiveConf                   hiveConf,
									  HiveAuthenticationProvider hiveAuthenticator,
									  HiveAuthzSessionContext    context) {
		mMetastoreClientFactory = metastoreClientFactory;
		mHiveConf               = hiveConf;
		mHiveAuthenticator      = hiveAuthenticator;
		mSessionContext         = context;

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

	public HiveAuthzSessionContext getHiveAuthzSessionContext() {
		return mSessionContext;
	}

	public UserGroupInformation getCurrentUserGroupInfo() {
		return mUgi;
	}
	
	public XaHiveAccessContext getAccessContext(HiveAuthzContext context) {
		return new XaHiveAccessContext(context, mSessionContext);
	}

	@Override
	public void applyAuthorizationConfigPolicy(HiveConf arg0) {
		// TODO Auto-generated method stub
	}

	/**
	 * Grant privileges for principals on the object
	 * @param hivePrincipals
	 * @param hivePrivileges
	 * @param hivePrivObject
	 * @param grantorPrincipal
	 * @param grantOption
	 * @throws HiveAuthzPluginException
	 * @throws HiveAccessControlException
	 */
	@Override
	public void grantPrivileges(List<HivePrincipal> hivePrincipals,
								List<HivePrivilege> hivePrivileges,
								HivePrivilegeObject hivePrivObject,
								HivePrincipal grantorPrincipal,
								boolean       grantOption)
	    throws HiveAuthzPluginException, HiveAccessControlException {
		// TODO Auto-generated method stub
	}

	/**
	 * Revoke privileges for principals on the object
	 * @param hivePrincipals
	 * @param hivePrivileges
	 * @param hivePrivObject
	 * @param grantorPrincipal
	 * @param grantOption
	 * @throws HiveAuthzPluginException
	 * @throws HiveAccessControlException
	 */
	@Override
	public void revokePrivileges(List<HivePrincipal> hivePrincipals,
								 List<HivePrivilege> hivePrivileges,
								 HivePrivilegeObject hivePrivObject,
								 HivePrincipal grantorPrincipal,
								 boolean       grantOption)
	    throws HiveAuthzPluginException, HiveAccessControlException {
		// TODO Auto-generated method stub
	}

	/**
	 * Show privileges for given principal on given object
	 * @param principal
	 * @param privObj
	 * @return
	 * @throws HiveAuthzPluginException
	 * @throws HiveAccessControlException
	 */
	@Override
	public List<HivePrivilegeInfo> showPrivileges(HivePrincipal principal, HivePrivilegeObject privObj)
	    throws HiveAuthzPluginException, HiveAccessControlException {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * Check if user has privileges to do this action on these objects
	 * @param hiveOpType
	 * @param inputsHObjs
	 * @param outputHObjs
	 * @param context
	 * @throws HiveAuthzPluginException
	 * @throws HiveAccessControlException
	 */
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
	public void grantRole(List<HivePrincipal> arg0, List<String> arg1,
			boolean arg2, HivePrincipal arg3) throws HiveAuthzPluginException,
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
}
