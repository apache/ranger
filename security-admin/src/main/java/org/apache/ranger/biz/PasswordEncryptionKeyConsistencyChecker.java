/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ranger.biz;

import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXServiceConfigMap;
import org.apache.ranger.plugin.util.PasswordUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

import java.util.List;

/**
 * RANGER-5773 (v1-&gt;v2 service-config password format): a v2-format value is decrypted using
 * whichever key THIS process has configured for {@code ranger.password.encryption.key} - unlike
 * legacy v1, the key is never stored with the value, so it is no longer self-describing. Before
 * this fix, a mismatched key across Admin nodes in an HA deployment was silently tolerated (each
 * node read the key back out of the row it was decrypting); after this fix, every node MUST agree
 * on the same key, or a node with a different key cannot decrypt service passwords that another
 * node in the same cluster wrote. Nothing enforced that agreement before this class - operators
 * would only discover a mismatch when a "test connection" or resource lookup unexpectedly failed
 * on one specific node, which is a hard failure mode to diagnose after the fact. (That failure
 * mode is no longer silent either - see RangerServiceService.getConfigsWithDecryptedPassword(),
 * which now throws an actionable error, with this same remediation text, at the point of use
 * instead of quietly leaving the "*****" mask in place.)
 * <p>
 * This check is deliberately opportunistic rather than authoritative: it does not introduce any
 * new stored state (no new table, no persisted fingerprint) - it simply tries to decrypt one
 * already-migrated v2 row, using whatever this node's configured key is, at startup. If some other
 * node already wrote v2 data under a different key - including a key that was rotated on some
 * nodes but not re-encrypted everywhere - this node will fail to decrypt it and log a loud,
 * actionable warning. It cannot catch every case (e.g. the very first node to write a v2 value
 * under a since-changed key, before any other node has restarted to notice) - it catches the
 * realistic, steady-state case: a node (re)starts and can't decrypt what the cluster already has.
 * <p>
 * Design decision flagged for reviewer sign-off, not silently assumed: this WARNS and lets startup
 * continue, rather than failing startup outright. A hard failure would guarantee the mismatch is
 * never missed, but risks turning a legitimate transient state (e.g. a deliberate key-rotation
 * window someone is midway through) into an outage. Kept as fail-open/warn-only to match this
 * fix's overall risk posture (see PatchServicePasswordV2Migration_J10067's per-row fail-soft
 * choice for the same reasoning) - worth a second opinion before this ships, not a unilateral call.
 * <p>
 * Looks up one v2-format row via a targeted, indexed-friendly {@code configvalue LIKE 'v2,%'}
 * query (XXServiceConfigMapDao.findByConfigValuePrefix(), LIMIT 1) rather than loading the entire
 * x_service_config_map table - unlike the migration patch, which genuinely needs every row (it
 * migrates all of them), this check only ever needs one, so there is no reason to pay for a full
 * scan on every Admin process startup.
 *
 * @Lazy(false) is required, not decorative: this context's applicationContext.xml sets
 * default-lazy-init="true" for every bean, and nothing else in the codebase ever autowires or
 * otherwise references this class - a startup-only checker is by design never looked up by
 * anything else. Under the context default, that combination means Spring would never construct
 * this bean at all, so @PostConstruct would silently never fire and this check would never run
 * in a real deployment. Confirmed by testing against a live docker environment: without this
 * annotation, checkKeyConsistency() produced zero log output at any level - not even the
 * fail-open outer catch - because the bean itself was never instantiated.
 */
@Lazy(value = false)
@Component
public class PasswordEncryptionKeyConsistencyChecker {
    private static final Logger LOG = LoggerFactory.getLogger(PasswordEncryptionKeyConsistencyChecker.class);

    @Autowired
    RangerDaoManager daoMgr;

    @PostConstruct
    public void checkKeyConsistency() {
        checkEncryptionKeyNotDefault();
        try {
            List<XXServiceConfigMap> v2ConfigMaps = daoMgr.getXXServiceConfigMap().findByConfigValuePrefix("v2,", 1);
            if (v2ConfigMaps.isEmpty()) {
                LOG.debug("Password encryption key consistency check skipped: no v2-format service config data found yet.");
                return;
            }
            XXServiceConfigMap configMap = v2ConfigMaps.get(0);
            try {
                PasswordUtils.decryptPasswordV2(configMap.getConfigvalue(), ServiceDBStore.ENCRYPT_KEY.toCharArray());
                LOG.debug("Password encryption key consistency check passed: this node can decrypt existing v2-format service config data.");
            } catch (Exception e) {
                // Deliberately do not log configMap.getConfigkey()'s value, the stored value, or
                // any part of the key - only enough to point an operator at the right service and
                // the right node.
                LOG.warn("This Admin node ({}, config path ranger-admin-site.xml) failed to decrypt an existing v2-format service config value " +
                                "(serviceId=[{}]) using its configured ranger.password.encryption.key. {}",
                        ServiceDBStore.LOCAL_HOSTNAME, configMap.getServiceId(), ServiceDBStore.ENCRYPT_KEY_MISMATCH_REMEDIATION, e);
            }
        } catch (Exception e) {
            // This check must never block Admin startup on its own account, even if it can't run at all.
            LOG.warn("Password encryption key consistency check could not run at startup - continuing without it.", e);
        }
    }

    /**
     * Loudly WARNs, once per Admin startup, if this node would encrypt (or already has encrypted)
     * service config passwords under the default, publicly-known key that ships with every Ranger
     * install - i.e. {@code ranger.password.encryption.key} was never actually set. Deliberately
     * WARN rather than a hard failure on every service create/update: the write path itself
     * (ServiceDBStore.createService()/updateService()) does not refuse a default key today, so
     * making startup itself fail here would not stop those writes anyway, only make the
     * misconfiguration harder to fix (an Admin that won't start can't have its config corrected
     * through its own UI/API). PatchServicePasswordV2Migration_J10067 is the one place in this fix
     * that DOES hard-refuse on a default/unset key (see PasswordUtils.validateEncryptionKeyConfigured())
     * - that migration is a deliberate, one-time, operator-triggered action with no legacy fallback
     * to preserve, unlike an interactive service create/update call.
     */
    private void checkEncryptionKeyNotDefault() {
        try {
            PasswordUtils.validateEncryptionKeyConfigured(ServiceDBStore.ENCRYPT_KEY.toCharArray());
        } catch (Exception e) {
            LOG.warn("This Admin node ({}, config path ranger-admin-site.xml) does not have ranger.password.encryption.key set to a real value - service " +
                            "config passwords are being encrypted (or already were) under the default, publicly-known key that ships with every Ranger " +
                            "install, which protects nothing. Set ranger.password.encryption.key to a unique, secret value as soon as possible - the same " +
                            "value on every Admin node in this cluster.",
                    ServiceDBStore.LOCAL_HOSTNAME);
        }
    }
}
