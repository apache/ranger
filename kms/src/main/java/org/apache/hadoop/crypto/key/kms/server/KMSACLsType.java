package org.apache.hadoop.crypto.key.kms.server;

public class KMSACLsType {

	public enum Type {
	    CREATE, DELETE, ROLLOVER, GET, GET_KEYS, GET_METADATA,
	    SET_KEY_MATERIAL, GENERATE_EEK, DECRYPT_EEK;

	    public String getAclConfigKey() {
	      return KMSConfiguration.CONFIG_PREFIX + "acl." + this.toString();
	    }

	    public String getBlacklistConfigKey() {
	      return KMSConfiguration.CONFIG_PREFIX + "blacklist." + this.toString();
	    }
	  }
}
