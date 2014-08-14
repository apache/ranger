package com.xasecure.authorization.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.xasecure.authorization.hadoop.config.XaSecureConfiguration;
import com.xasecure.authorization.hadoop.constants.XaSecureHadoopConstants;

public class XaHiveAccessVerifierFactory {

	private static final Log LOG = LogFactory.getLog(XaHiveAccessVerifierFactory.class) ;

	private static XaHiveAccessVerifier hiveAccessVerififer = null ;
	
	public static XaHiveAccessVerifier getInstance() {
		if (hiveAccessVerififer == null) {
			synchronized(XaHiveAccessVerifierFactory.class) {
				XaHiveAccessVerifier temp = hiveAccessVerififer ;
				if (temp == null) {
					String hiveAccessVerifierClassName = XaSecureConfiguration.getInstance().get(XaSecureHadoopConstants.HIVE_ACCESS_VERIFIER_CLASS_NAME_PROP, XaSecureHadoopConstants.HIVE_ACCESS_VERIFIER_CLASS_NAME_DEFAULT_VALUE ) ;

					if (hiveAccessVerifierClassName != null) {
						LOG.info("Hive Access Verification class [" + hiveAccessVerifierClassName + "] - Being built");
						try {
							hiveAccessVerififer = (XaHiveAccessVerifier) (Class.forName(hiveAccessVerifierClassName).newInstance()) ;
							LOG.info("Created a new instance of class: [" + hiveAccessVerifierClassName + "] for Hive Access verification.");
						} catch (InstantiationException e) {
							LOG.error("Unable to create HiveAccess Verifier: [" +  hiveAccessVerifierClassName + "]", e);
						} catch (IllegalAccessException e) {
							LOG.error("Unable to create HiveAccess Verifier: [" +  hiveAccessVerifierClassName + "]", e);
						} catch (ClassNotFoundException e) {
							LOG.error("Unable to create HiveAccess Verifier: [" +  hiveAccessVerifierClassName + "]", e);
						} catch (Throwable t) {
							LOG.error("Unable to create HiveAccess Verifier: [" +  hiveAccessVerifierClassName + "]", t);
						}
						finally {
							LOG.info("Created a new instance of class: [" + hiveAccessVerifierClassName + "] for Hive Access verification. (" + hiveAccessVerififer + ")");
						}
					}
				}
				else {
					LOG.error("Unable to obtain hiveAccessVerifier [" +  XaSecureHadoopConstants.HIVE_ACCESS_VERIFIER_CLASS_NAME_PROP + "]");
				}
			}
		}
		return hiveAccessVerififer ;
	}
}
