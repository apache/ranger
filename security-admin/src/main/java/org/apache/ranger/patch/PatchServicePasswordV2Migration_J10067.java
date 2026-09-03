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
package org.apache.ranger.patch;

import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.biz.ServiceDBStore;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.entity.XXServiceConfigMap;
import org.apache.ranger.plugin.util.PasswordUtils;
import org.apache.ranger.plugin.util.RangerSupportedCryptoAlgo;
import org.apache.ranger.util.CLIUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * One-time upgrade migration for RANGER-5773 (service-config credential encryption key was
 * stored alongside its own ciphertext). The code-level fix (see PasswordUtils.encryptPasswordV2/
 * decryptPasswordV2 and their callers in ServiceDBStore/RangerServiceService) only changes how
 * *new* writes are stored — every password already in x_service_config_map before this patch
 * runs still carries the vulnerable v1 format, with the key embedded in the same row as its
 * ciphertext. This patch is what actually closes that exposure for existing data: it decrypts
 * every password-type config value still in the legacy format and re-writes it in the v2 format
 * (key sourced from configuration, never stored).
 * <p>
 * Modeled on {@link PatchPasswordEncryption_J10001}, which did the equivalent migration for the
 * legacy x_asset config column when password encryption was first introduced.
 * <p>
 * Design notes worth a reviewer's attention rather than being silently assumed:
 * - Single-pass, single-transaction, like PatchPasswordEncryption_J10001 (batchSize is not set,
 * so BaseLoader runs execLoad() exactly once and commits at the end). For a very large
 * x_service_config_map this is a real limitation (lock/timeout risk) — matches existing
 * precedent, but is worth a second opinion on very large deployments before this ships,
 * rather than assuming precedent alone settles it.
 * - Per-row failures are caught and logged, NOT allowed to abort the whole patch — this covers
 * the full per-row body (this row's XXService/service-def lookups included, not just the
 * decrypt/re-encrypt/write step) precisely so a transient DAO issue on one row can't do it
 * either. This is a deliberate improvement over the precedent (which has no per-row error
 * handling at all, so a single bad row would abort the entire migration via BaseLoader's
 * top-level catch). A row that fails to migrate is simply left in v1 format — still fully
 * functional, just not yet protected — rather than blocking every other row from being
 * migrated.
 * - Idempotent / safe to re-run: rows already in v2 format (isV2Format()) are skipped, so this
 * patch can be re-run (e.g. after fixing a config issue that caused failures) without
 * re-encrypting already-migrated rows.
 * - Config (key + algorithm) is validated ONCE up front, before any row is touched — see
 * {@link #validateMigrationConfig()} — rather than letting a bad config burn every single row
 * as its own per-row failure with the same root cause.
 * - A row is only treated as "legacy-format, needs migrating" when {@code
 * PasswordUtils.isLegacyFormat()} recognizes it (first field names a real crypto algorithm),
 * not merely because its stored value contains a comma. The old, looser
 * {@code storedValue.contains(",")} heuristic could misclassify a plaintext password that
 * happens to contain a comma as "already encrypted" and attempt to decrypt/re-encrypt it —
 * rows that don't match either recognized format are counted as notEncrypted and left alone.
 */
@Component
public class PatchServicePasswordV2Migration_J10067 extends BaseLoader {
    private static final Logger logger = LoggerFactory.getLogger(PatchServicePasswordV2Migration_J10067.class);

    @Autowired
    RangerDaoManager daoMgr;

    int lineCount;
    int migratedCount;
    int alreadyV2Count;
    int notPasswordCount;
    int notEncryptedCount;
    int orphanedCount;
    int failedCount;

    public PatchServicePasswordV2Migration_J10067() {
    }

    public static void main(String[] args) {
        logger.info("main()");

        try {
            PatchServicePasswordV2Migration_J10067 loader = (PatchServicePasswordV2Migration_J10067) CLIUtil.getBean(PatchServicePasswordV2Migration_J10067.class);
            loader.init();
            while (loader.isMoreToProcess()) {
                loader.load();
            }
            logger.info("Load complete. Exiting!!!");
            System.exit(0);
        } catch (Exception e) {
            logger.error("Error loading", e);
            System.exit(1);
        }
    }

    @Override
    public void printStats() {
        logger.info("Time taken so far:{}, moreToProcess={}", timeTakenSoFar(lineCount), isMoreToProcess());
        print(lineCount, "Processed config rows");
    }

    @Override
    public void execLoad() {
        migrateServicePasswordsToV2();
    }

    void migrateServicePasswordsToV2() {
        validateMigrationConfig(); // fail fast, once, with one clear message — see javadoc below
        List<XXServiceConfigMap> allConfigMaps = daoMgr.getXXServiceConfigMap().getAll();
        Map<Long, Set<String>> passwordConfigKeysByServiceDefId = new HashMap<>();

        for (XXServiceConfigMap configMap : allConfigMaps) {
            lineCount++;

            String configValue = configMap.getConfigvalue();
            boolean isV2 = PasswordUtils.isV2Format(configValue);

            if (!isV2 && !PasswordUtils.isLegacyFormat(configValue)) {
                // Neither format is recognized — plaintext, some other unrelated config value, or
                // (previously, under the old ".contains(\",\")" heuristic) a plaintext value that
                // merely happened to contain a comma. isLegacyFormat() requires the first field to
                // actually name a supported crypto algorithm, so a stray comma alone no longer
                // routes a row into decrypt/re-encrypt/failedCount below.
                notEncryptedCount++;
                continue;
            }

            // Everything below is per-row and deliberately inside one try/catch, not just the
            // crypto/write step at the bottom - a row-specific DAO lookup failure here (e.g. a
            // transient issue resolving this row's XXService or its service-def's password config
            // keys) must not be allowed to abort the whole patch and roll back every row already
            // migrated in this run, any more than a decrypt/encrypt failure should. Only
            // daoMgr.getXXServiceConfigMap().getAll() above stays outside - that one operation
            // isn't "per-row", there's no partial-row granularity to preserve if it fails.
            XXService xService = null;

            try {
                xService = daoMgr.getXXService().getById(configMap.getServiceId());

                if (xService == null) {
                    logger.warn("Skipping config row [{}] — no service found for serviceId [{}] (orphaned row)", configMap.getId(), configMap.getServiceId());
                    orphanedCount++;
                    continue;
                }

                Set<String> passwordConfigKeys = passwordConfigKeysByServiceDefId.computeIfAbsent(xService.getType(),
                        serviceDefId -> ServiceDBStore.getPasswordConfigKeys(daoMgr.getXXServiceConfigDef().findConfigNamesByServiceDefIdAndType(serviceDefId, ServiceDBStore.CONFIG_TYPE_PASSWORD)));

                if (!ServiceDBStore.isPasswordConfigKey(passwordConfigKeys, configMap.getConfigkey())) {
                    notPasswordCount++;
                    continue;
                }

                if (isV2) {
                    alreadyV2Count++;
                    continue; // already migrated — safe to re-run this patch
                }

                String newStoredValue = migrateValue(configValue, RangerSupportedCryptoAlgo.getValueOf(ServiceDBStore.CRYPT_ALGO),
                        ServiceDBStore.ENCRYPT_KEY.toCharArray(), ServiceDBStore.SALT.getBytes(StandardCharsets.UTF_8), ServiceDBStore.ITERATION_COUNT);

                configMap.setConfigvalue(newStoredValue);

                daoMgr.getXXServiceConfigMap().update(configMap);

                migratedCount++;
            } catch (Exception e) {
                // Leave this row in v1 format — it keeps working via the untouched legacy
                // decrypt path; it just isn't protected by this fix until it's retried. Applies
                // equally whether the failure was the crypto/write step or an earlier lookup for
                // this row - either way, only this one row is affected.
                String serviceLabel = xService != null ? xService.getName() : ("serviceId=" + configMap.getServiceId());

                logger.error("Failed to migrate password to v2 for service [{}], configKey [{}] — row left in legacy format", serviceLabel, configMap.getConfigkey(), e);

                failedCount++;
            }
        }

        setMoreToProcess(false);

        logger.info("Password v1->v2 migration complete: migrated={}, alreadyV2={}, notPasswordConfig={}, notEncrypted={}, orphaned={}, failed={} (total rows seen={})",
                migratedCount, alreadyV2Count, notPasswordCount, notEncryptedCount, orphanedCount, failedCount, lineCount);
    }

    /**
     * Fails the whole patch run immediately, before touching a single row, if this node's
     * password-encryption configuration can't actually produce valid v2 output — an unset/default
     * {@code ranger.password.encryption.key}, or a {@code ranger.password.encryption.algorithm}
     * value that doesn't name a supported algorithm. Without this up-front check, a bad config
     * doesn't fail cleanly: every single candidate row fails individually inside the per-row
     * try/catch in {@link #migrateServicePasswordsToV2()}, each logged as its own "Failed to
     * migrate password..." error with the same root cause, and failedCount ends up misleadingly
     * large. A configuration problem should stop the whole patch with ONE clear, actionable
     * message instead of N confusing per-row ones.
     */
    private static void validateMigrationConfig() {
        PasswordUtils.validateEncryptionKeyConfigured(ServiceDBStore.ENCRYPT_KEY.toCharArray());

        RangerSupportedCryptoAlgo.getValueOf(ServiceDBStore.CRYPT_ALGO); // throws if not a supported algorithm name
    }

    /**
     * Pure migration logic for one stored value, split out from the DAO/Spring plumbing above so
     * it can be exercised directly in a test: decrypt via the legacy (self-contained) path,
     * re-encrypt via v2 with the supplied key, and verify the round-trip before trusting it.
     * Throws on any failure (wrong/missing key, corrupt data, decrypt/encrypt error) — callers
     * must treat that as "leave the row unmigrated," never as a signal to fall back silently.
     */
    static String migrateValue(String legacyStoredValue, RangerSupportedCryptoAlgo cryptAlgo, char[] key, byte[] salt, int iterationCount) throws Exception {
        String decryptedPwd = PasswordUtils.decryptPassword(legacyStoredValue);
        String newStoredValue = PasswordUtils.encryptPasswordV2(decryptedPwd, cryptAlgo, key, salt, iterationCount);
        String verifyDecrypted = PasswordUtils.decryptPasswordV2(newStoredValue, key);

        if (!StringUtils.equals(decryptedPwd, verifyDecrypted)) {
            throw new IllegalStateException("v2 round-trip verification did not reproduce the original decrypted value");
        }

        return newStoredValue;
    }
}
