package com.xasecure.authorization.hive.authorizer;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizer;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizerFactory;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveMetastoreClientFactory;

public class XaSecureHiveAuthorizerFactory implements HiveAuthorizerFactory {
	@Override
	public HiveAuthorizer createHiveAuthorizer(HiveMetastoreClientFactory metastoreClientFactory,
											   HiveConf conf,
											   HiveAuthenticationProvider hiveAuthenticator)
													   throws HiveAuthzPluginException {
		return new XaSecureHiveAuthorizer(metastoreClientFactory, conf, hiveAuthenticator);
	}
}
