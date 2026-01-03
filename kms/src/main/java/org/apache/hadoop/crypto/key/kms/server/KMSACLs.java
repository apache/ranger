/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.crypto.key.kms.server;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.kms.server.KMS.KMSOp;
import org.apache.hadoop.crypto.key.kms.server.KMSACLsType.Type;
import org.apache.hadoop.crypto.key.kms.server.KeyAuthorizationKeyProvider.KeyACLs;
import org.apache.hadoop.crypto.key.kms.server.KeyAuthorizationKeyProvider.KeyOpType;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Provides access to the <code>AccessControlList</code>s used by KMS,
 * hot-reloading them if the <code>dbks-site.xml</code> file where the ACLs
 * are defined has been updated.
 */
@InterfaceAudience.Private
public class KMSACLs implements Runnable, KeyACLs {
    private static final Logger LOG = LoggerFactory.getLogger(KMSACLs.class);

    public  static final String ACL_DEFAULT                  = AccessControlList.WILDCARD_ACL_VALUE;
    public  static final int    RELOADER_SLEEP_MILLIS        = 1000;
    private static final String UNAUTHORIZED_MSG_WITH_KEY    = "User:%s not allowed to do '%s' on '%s'";
    private static final String UNAUTHORIZED_MSG_WITHOUT_KEY = "User:%s not allowed to do '%s'";

    @VisibleForTesting
    volatile Map<String, HashMap<KeyOpType, AccessControlList>> keyAcls;

    @VisibleForTesting
    volatile Map<KeyOpType, AccessControlList> defaultKeyAcls = new HashMap<>();

    @VisibleForTesting
    volatile Map<KeyOpType, AccessControlList> whitelistKeyAcls = new HashMap<>();

    private volatile Map<Type, AccessControlList> acls;
    private volatile Map<Type, AccessControlList> blacklistedAcls;
    private          ScheduledExecutorService     executorService;
    private          long                         lastReload;

    KMSACLs(Configuration conf) {
        if (conf == null) {
            conf = loadACLs();
        }

        setKMSACLs(conf);
        setKeyACLs(conf);
    }

    public KMSACLs() {
        this(null);
    }

    @Override
    public void run() {
        try {
            if (KMSConfiguration.isACLsFileNewer(lastReload)) {
                setKMSACLs(loadACLs());
                setKeyACLs(loadACLs());
            }
        } catch (Exception ex) {
            LOG.warn("Could not reload ACLs file: '{}'", ex, ex);
        }
    }

    @Override
    public boolean hasAccessToKey(String keyName, UserGroupInformation ugi, KeyOpType opType) {
        boolean access = checkKeyAccess(keyName, ugi, opType) || checkKeyAccess(whitelistKeyAcls, ugi, opType);

        if (!access) {
            KMSWebApp.getKMSAudit().unauthorized(ugi, opType, keyName);
        }

        return access;
    }

    @Override
    public boolean isACLPresent(String keyName, KeyOpType opType) {
        return (keyAcls.containsKey(keyName) || defaultKeyAcls.containsKey(opType));
    }

    @Override
    public synchronized void startReloader() {
        if (executorService == null) {
            executorService = Executors.newScheduledThreadPool(1);

            executorService.scheduleAtFixedRate(this, RELOADER_SLEEP_MILLIS, RELOADER_SLEEP_MILLIS, TimeUnit.MILLISECONDS);
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
    public boolean hasAccess(Type type, UserGroupInformation ugi, String clientIp) {
        boolean access = acls.get(type).isUserAllowed(ugi);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Checking user [{}] for: {} {} ", ugi.getShortUserName(), type, acls.get(type).getAclString());
        }

        if (access) {
            AccessControlList blacklist = blacklistedAcls.get(type);

            access = (blacklist == null) || !blacklist.isUserInList(ugi);

            if (LOG.isDebugEnabled()) {
                if (blacklist == null) {
                    LOG.debug("No blacklist for {}", type);
                } else if (access) {
                    LOG.debug("user is not in {}", blacklist.getAclString());
                } else {
                    LOG.debug("user is in {}", blacklist.getAclString());
                }
            }
        }

        LOG.debug("User: [{}], Type: {} Result: {}", ugi.getShortUserName(), type, access);

        return access;
    }

    @Override
    public void assertAccess(Type aclType, UserGroupInformation ugi, KMSOp operation, String key, String clientIp) throws AccessControlException {
        if (!KMSWebApp.getACLs().hasAccess(aclType, ugi, clientIp)) {
            KMSWebApp.getUnauthorizedCallsMeter().mark();
            KMSWebApp.getKMSAudit().unauthorized(ugi, operation, key);

            throw new AuthorizationException(String.format((key != null) ? UNAUTHORIZED_MSG_WITH_KEY : UNAUTHORIZED_MSG_WITHOUT_KEY, ugi.getShortUserName(), operation, key));
        }
    }

    @VisibleForTesting
    void setKeyACLs(Configuration conf) {
        Map<String, HashMap<KeyOpType, AccessControlList>> tempKeyAcls = new HashMap<>();
        Map<String, String>                                allKeyACLS  = conf.getValByRegex(KMSConfiguration.KEY_ACL_PREFIX_REGEX);

        for (Map.Entry<String, String> keyAcl : allKeyACLS.entrySet()) {
            String k             = keyAcl.getKey(); // this should be of type "key.acl.<KEY_NAME>.<OP_TYPE>"
            int    keyNameStarts = KMSConfiguration.KEY_ACL_PREFIX.length();
            int    keyNameEnds   = k.lastIndexOf(".");

            if (keyNameStarts >= keyNameEnds) {
                LOG.warn("Invalid key name '{}'", k);
            } else {
                String    aclStr  = keyAcl.getValue();
                String    keyName = k.substring(keyNameStarts, keyNameEnds);
                String    keyOp   = k.substring(keyNameEnds + 1);
                KeyOpType aclType = null;

                try {
                    aclType = KeyOpType.valueOf(keyOp);
                } catch (IllegalArgumentException e) {
                    LOG.warn("Invalid key Operation '{}'", keyOp);
                }

                if (aclType != null) {
                    // On the assumption this will be single threaded.. else we need to ConcurrentHashMap
                    HashMap<KeyOpType, AccessControlList> aclMap = tempKeyAcls.computeIfAbsent(keyName, k1 -> new HashMap<>());

                    aclMap.put(aclType, new AccessControlList(aclStr));

                    LOG.info("KEY_NAME '{}' KEY_OP '{}' ACL '{}'", keyName, aclType, aclStr);
                }
            }
        }

        keyAcls = tempKeyAcls;

        final Map<KeyOpType, AccessControlList> tempDefaults   = new HashMap<>();
        final Map<KeyOpType, AccessControlList> tempWhitelists = new HashMap<>();

        for (KeyOpType keyOp : KeyOpType.values()) {
            parseAclsWithPrefix(conf, KMSConfiguration.DEFAULT_KEY_ACL_PREFIX, keyOp, tempDefaults);
            parseAclsWithPrefix(conf, KMSConfiguration.WHITELIST_KEY_ACL_PREFIX, keyOp, tempWhitelists);
        }

        defaultKeyAcls   = tempDefaults;
        whitelistKeyAcls = tempWhitelists;
    }

    private void setKMSACLs(Configuration conf) {
        Map<Type, AccessControlList> tempAcls      = new HashMap<>();
        Map<Type, AccessControlList> tempBlacklist = new HashMap<>();

        for (Type aclType : Type.values()) {
            String aclStr = conf.get(aclType.getAclConfigKey(), ACL_DEFAULT);

            tempAcls.put(aclType, new AccessControlList(aclStr));

            String blacklistStr = conf.get(aclType.getBlacklistConfigKey());

            if (blacklistStr != null) {
                // Only add if blacklist is present
                tempBlacklist.put(aclType, new AccessControlList(blacklistStr));

                LOG.info("'{}' Blacklist '{}'", aclType, blacklistStr);
            }

            LOG.info("'{}' ACL '{}'", aclType, aclStr);
        }

        acls            = tempAcls;
        blacklistedAcls = tempBlacklist;
    }

    /**
     * Parse the acls from configuration with the specified prefix. Currently
     * only 2 possible prefixes: whitelist and default.
     *
     * @param conf The configuration.
     * @param prefix The prefix.
     * @param keyOp The key operation.
     * @param results The collection of results to add to.
     */
    private void parseAclsWithPrefix(final Configuration conf, final String prefix, final KeyOpType keyOp, Map<KeyOpType, AccessControlList> results) {
        String confKey = prefix + keyOp;
        String aclStr  = conf.get(confKey);

        if (aclStr != null) {
            if (keyOp == KeyOpType.ALL) {
                // Ignore All operation for default key and whitelist key acls
                LOG.warn("Invalid KEY_OP '{}' for {}, ignoring", keyOp, prefix);
            } else {
                if (aclStr.equals("*")) {
                    LOG.info("{} for KEY_OP '{}' is set to '*'", prefix, keyOp);
                }

                results.put(keyOp, new AccessControlList(aclStr));
            }
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

    private boolean checkKeyAccess(String keyName, UserGroupInformation ugi, KeyOpType opType) {
        Map<KeyOpType, AccessControlList> keyAcl = keyAcls.get(keyName);

        if (keyAcl == null) {
            // If No key acl defined for this key, check to see if
            // there are key defaults configured for this operation
            LOG.debug("Key: {} has no ACLs defined, using defaults.", keyName);

            keyAcl = defaultKeyAcls;
        }

        boolean access = checkKeyAccess(keyAcl, ugi, opType);

        LOG.debug("User: [{}], OpType: {}, KeyName: {} Result: {}", ugi.getShortUserName(), opType, keyName, access);

        return access;
    }

    private boolean checkKeyAccess(Map<KeyOpType, AccessControlList> keyAcl, UserGroupInformation ugi, KeyOpType opType) {
        AccessControlList acl = keyAcl.get(opType);

        if (acl == null) {
            // If no acl is specified for this operation, deny access
            LOG.debug("No ACL available for key, denying access for {}", opType);

            return false;
        } else {
            LOG.debug("Checking user [{}] for: {}: {}", ugi.getShortUserName(), opType, acl.getAclString());

            return acl.isUserAllowed(ugi);
        }
    }
}
