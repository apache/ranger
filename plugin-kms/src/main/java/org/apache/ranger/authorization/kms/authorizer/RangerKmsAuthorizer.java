
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.ranger.authorization.kms.authorizer;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.kms.server.KMSACLsType;
import org.apache.hadoop.crypto.key.kms.server.KMSConfiguration;
import org.apache.hadoop.crypto.key.kms.server.KMSWebApp;
import org.apache.hadoop.crypto.key.kms.server.KMS.KMSOp;
import org.apache.hadoop.crypto.key.kms.server.KMSACLsType.Type;
import org.apache.hadoop.crypto.key.kms.server.KeyAuthorizationKeyProvider.KeyACLs;
import org.apache.hadoop.crypto.key.kms.server.KeyAuthorizationKeyProvider.KeyOpType;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.ranger.authorization.utils.StringUtil;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

public class RangerKmsAuthorizer implements Runnable, KeyACLs {
	  private static final Logger LOG = LoggerFactory.getLogger(RangerKmsAuthorizer.class);

	  private static final String UNAUTHORIZED_MSG_WITH_KEY =
	      "User:%s not allowed to do '%s' on '%s'";

	  private static final String UNAUTHORIZED_MSG_WITHOUT_KEY =
	      "User:%s not allowed to do '%s'";

	  public static final int RELOADER_SLEEP_MILLIS = 1000;
	  
	  private volatile Map<Type, AccessControlList> blacklistedAcls;
	  
	  private long lastReload;

	  private ScheduledExecutorService executorService;
	  
	  public static final String ACCESS_TYPE_DECRYPT_EEK   	= "decrypteek";
	  public static final String ACCESS_TYPE_GENERATE_EEK   = "generateeek";
	  public static final String ACCESS_TYPE_GET_METADATA  	= "getmetadata";
	  public static final String ACCESS_TYPE_GET_KEYS       = "getkeys";
	  public static final String ACCESS_TYPE_GET       		= "get";
	  public static final String ACCESS_TYPE_SET_KEY_MATERIAL= "setkeymaterial";
	  public static final String ACCESS_TYPE_ROLLOVER       = "rollover";
	  public static final String ACCESS_TYPE_CREATE       	= "create";
	  public static final String ACCESS_TYPE_DELETE       	= "delete";	  

	  private static volatile RangerKMSPlugin kmsPlugin = null;

	  RangerKmsAuthorizer(Configuration conf) {
		  if (conf == null) {
		      conf = loadACLs();		      
		  }
		  setKMSACLs(conf);	
		  init(conf);
	  }

	  public RangerKmsAuthorizer() {		  
	    this(null);
	  }
	  
	  @Override
	  public void run() {
		  try {
		      if (KMSConfiguration.isACLsFileNewer(lastReload)) {
		        setKMSACLs(loadACLs());
		      }
		    } catch (Exception ex) {
		      LOG.warn(
		          String.format("Could not reload ACLs file: '%s'", ex.toString()), ex);
		  }
	  }
	  
	  private Configuration loadACLs() {
		  LOG.debug("Loading ACLs file");
		  lastReload = System.currentTimeMillis();
		  Configuration conf = KMSConfiguration.getACLsConf();
		  // triggering the resource loading.
		  conf.get(Type.CREATE.getAclConfigKey());
		  return conf;
	  }

	  @Override
	  public synchronized void startReloader() {
	    if (executorService == null) {
	      executorService = Executors.newScheduledThreadPool(1);
	      executorService.scheduleAtFixedRate(this, RELOADER_SLEEP_MILLIS,
	          RELOADER_SLEEP_MILLIS, TimeUnit.MILLISECONDS);
	    }
	  }
	  @Override
	  public synchronized void stopReloader() {
	    if (executorService != null) {
	      executorService.shutdownNow();
	      executorService = null;
	    }
	  }

	  /**
	   * First Check if user is in ACL for the KMS operation, if yes, then
	   * return true if user is not present in any configured blacklist for
	   * the operation
	   * @param type KMS Operation
	   * @param ugi UserGroupInformation of user
	   * @return true is user has access
	   */
	  @Override
	  public boolean hasAccess(Type type, UserGroupInformation ugi) {
		  if(LOG.isDebugEnabled()) {
				LOG.debug("==> RangerKmsAuthorizer.hasAccess(" + type + ", " + ugi + ")");
			}

			boolean ret = false;
			RangerKMSPlugin plugin = kmsPlugin;
			String rangerAccessType = getRangerAccessType(type);
			AccessControlList blacklist = blacklistedAcls.get(type);
		    ret = (blacklist == null) || !blacklist.isUserInList(ugi);
		    if(!ret){
		    	LOG.debug("Operation "+rangerAccessType+" blocked in the blacklist for user "+ugi.getUserName());
		    }
		    
			if(plugin != null && ret) {				
				RangerKMSAccessRequest request = new RangerKMSAccessRequest(rangerAccessType, ugi);
				RangerAccessResult result = plugin.isAccessAllowed(request);
				ret = result == null ? false : result.getIsAllowed();
			}
			
			if(LOG.isDebugEnabled()) {
				LOG.debug("<== RangerkmsAuthorizer.hasAccess(" + type + ", " + ugi + "): " + ret);
			}

			return ret;
	  }

	  @Override
	  public void assertAccess(Type aclType, UserGroupInformation ugi, KMSOp operation, String key)
	      throws AccessControlException {
	    if (!KMSWebApp.getACLs().hasAccess(aclType, ugi)) {
	      KMSWebApp.getUnauthorizedCallsMeter().mark();
	      KMSWebApp.getKMSAudit().unauthorized(ugi, operation, key);
	      throw new AuthorizationException(String.format(
	          (key != null) ? UNAUTHORIZED_MSG_WITH_KEY
	                        : UNAUTHORIZED_MSG_WITHOUT_KEY,
	          ugi.getShortUserName(), operation, key));
	    }
	  }

	  @Override
	  public boolean hasAccessToKey(String keyName, UserGroupInformation ugi, KeyOpType opType) {
		  if(LOG.isDebugEnabled()) {
				LOG.debug("==> RangerKmsAuthorizer.hasAccessToKey(" + keyName + ", " + ugi +", " + opType + ")");
			}
			if(LOG.isDebugEnabled()) {
				LOG.debug("<== RangerKmsAuthorizer.hasAccessToKey(" + keyName + ", " + ugi +", " + opType + ")");
			}

			return true;
	 }

	  @Override
	  public boolean isACLPresent(String keyName, KeyOpType opType) {
	 	  return true;
	  }

   	  public void init(Configuration conf) {
			if(LOG.isDebugEnabled()) {
				LOG.debug("==> RangerKmsAuthorizer.init()");
			}

			RangerKMSPlugin plugin = kmsPlugin;

			if(plugin == null) {
				synchronized(RangerKmsAuthorizer.class) {
					plugin = kmsPlugin;

					if(plugin == null) {
						plugin = new RangerKMSPlugin();
						plugin.init();
						
						kmsPlugin = plugin;
					}
				}
			}
			if(LOG.isDebugEnabled()) {
				LOG.debug("<== RangerkmsAuthorizer.init()");
			}
		}
		
		private void setKMSACLs(Configuration conf) {
		    Map<Type, AccessControlList> tempBlacklist = new HashMap<Type, AccessControlList>();
		    for (Type aclType : Type.values()) {
			      String blacklistStr = conf.get(aclType.getBlacklistConfigKey());
			      if (blacklistStr != null) {
			        // Only add if blacklist is present
			        tempBlacklist.put(aclType, new AccessControlList(blacklistStr));
			        LOG.info("'{}' Blacklist '{}'", aclType, blacklistStr);
			      }
			    }
			    blacklistedAcls = tempBlacklist;
		}

		private static String getRangerAccessType(KMSACLsType.Type accessType) {
			String ret = null;
		
			switch(accessType) {
				case CREATE:
					ret = RangerKmsAuthorizer.ACCESS_TYPE_CREATE;
				break;

				case DELETE:
					ret = RangerKmsAuthorizer.ACCESS_TYPE_DELETE;
				break;
				
				case ROLLOVER:
					ret = RangerKmsAuthorizer.ACCESS_TYPE_ROLLOVER;
				break;
				
				case GET:
					ret = RangerKmsAuthorizer.ACCESS_TYPE_GET;
				break;
				
				case GET_KEYS:
					ret = RangerKmsAuthorizer.ACCESS_TYPE_GET_KEYS;
				break;
				
				case GET_METADATA:
					ret = RangerKmsAuthorizer.ACCESS_TYPE_GET_METADATA;
				break;
				
				case SET_KEY_MATERIAL:
					ret = RangerKmsAuthorizer.ACCESS_TYPE_SET_KEY_MATERIAL;
				break;
				
				case GENERATE_EEK:
					ret = RangerKmsAuthorizer.ACCESS_TYPE_GENERATE_EEK;
				break;
				
				case DECRYPT_EEK:
					ret = RangerKmsAuthorizer.ACCESS_TYPE_DECRYPT_EEK;
				break;			
			}
			return ret;
		}
	}

	class RangerKMSPlugin extends RangerBasePlugin {
		public RangerKMSPlugin() {
			super("kms", "kms");
		}

		@Override
		public void init() {
			super.init();

			RangerDefaultAuditHandler auditHandler = new RangerDefaultAuditHandler();

			super.setDefaultAuditHandler(auditHandler);
		}
	}

	class RangerKMSResource extends RangerAccessResourceImpl {
		private static final String KEY_NAME = "keyname";

		public RangerKMSResource(String keyname) {			
			setValue(KEY_NAME, keyname != null ? keyname : null);
		}
	}

	class RangerKMSAccessRequest extends RangerAccessRequestImpl {
		public RangerKMSAccessRequest(String accessType, UserGroupInformation ugi) {
			super.setResource(new RangerKMSResource("kms"));
			super.setAccessType(accessType);
			super.setUser(ugi.getShortUserName());
			super.setUserGroups(Sets.newHashSet(ugi.getGroupNames()));
			super.setAccessTime(StringUtil.getUTCDate());
			super.setClientIPAddress(getRemoteIp());
			super.setAction(accessType);
		}
		
		private static String getRemoteIp() {
			String ret = null ;
			InetAddress ip = Server.getRemoteIp() ;
			if (ip != null) {
				ret = ip.getHostAddress();
			}
			return ret ;
		}
	}
