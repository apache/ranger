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
package org.apache.ranger.db.upgrade.patches.kms;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import liquibase.change.custom.CustomTaskChange;
import liquibase.change.custom.CustomTaskRollback;
import liquibase.database.Database;
import liquibase.exception.CustomChangeException;
import liquibase.exception.SetupException;
import liquibase.exception.ValidationErrors;
import liquibase.resource.ResourceAccessor;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.RangerKMSDB;
import org.apache.hadoop.crypto.key.RangerKeyStoreProvider;
import org.apache.ranger.entity.XXRangerKeyStore;
import org.apache.ranger.kms.dao.DaoManager;
import org.apache.ranger.kms.dao.RangerKMSDao;

import java.util.List;
import java.util.Map;

@SuppressWarnings("PMD.JUnit4TestShouldUseBeforeAnnotation")
public class UpdateKMSAttrs implements CustomTaskChange, CustomTaskRollback {
    @Override
    public void execute(Database database) throws CustomChangeException {
        try {
            Gson                   gson               = new GsonBuilder().create();
            Configuration          conf               = RangerKeyStoreProvider.getDBKSConf();
            RangerKMSDB            rangerKMSDB        = new RangerKMSDB(conf);
            DaoManager             daoManager         = rangerKMSDB.getDaoManager();
            RangerKMSDao           kmsDao             = daoManager.getRangerKMSDao();
            List<XXRangerKeyStore> rangerKeyStoreList = kmsDao.getAllKeys();
            for (XXRangerKeyStore xxRangerKeyStore : rangerKeyStoreList) {
                String              kmsAttrsStr = xxRangerKeyStore.getAttributes();
                Map<String, String> kmsAttrs    = gson.fromJson(kmsAttrsStr, Map.class);
                if (MapUtils.isNotEmpty(kmsAttrs)) {
                    String kmsAclName = kmsAttrs.get("key.acl.name");
                    if (StringUtils.isNotEmpty(kmsAclName)) {
                        kmsAttrs.put("key.acl.new.name", kmsAclName);
                        //kmsAttrs.remove("key.acl.name"); -- For backward compatibility
                        kmsAttrsStr = gson.toJson(kmsAttrs);
                        xxRangerKeyStore.setAttributes(kmsAttrsStr);
                        kmsDao.update(xxRangerKeyStore);
                    }
                }
            }
        } catch (Exception e) {
            throw new CustomChangeException(e);
        }
    }

    @Override
    public void rollback(Database database) throws CustomChangeException {
        try {
            Gson                   gson               = new GsonBuilder().create();
            Configuration          conf               = RangerKeyStoreProvider.getDBKSConf();
            RangerKMSDB            rangerKMSDB        = new RangerKMSDB(conf);
            DaoManager             daoManager         = rangerKMSDB.getDaoManager();
            RangerKMSDao           kmsDao             = daoManager.getRangerKMSDao();
            List<XXRangerKeyStore> rangerKeyStoreList = kmsDao.getAllKeys();
            for (XXRangerKeyStore xxRangerKeyStore : rangerKeyStoreList) {
                String              kmsAttrsStr = xxRangerKeyStore.getAttributes();
                Map<String, String> kmsAttrs    = gson.fromJson(kmsAttrsStr, Map.class);
                if (MapUtils.isNotEmpty(kmsAttrs)) {
                    String kmsAclName = kmsAttrs.get("key.acl.new.name");
                    if (StringUtils.isNotEmpty(kmsAclName)) {
                        kmsAttrs.remove("key.acl.new.name");
                        kmsAttrsStr = gson.toJson(kmsAttrs);
                        xxRangerKeyStore.setAttributes(kmsAttrsStr);
                        kmsDao.update(xxRangerKeyStore);
                    }
                }
            }
        } catch (Exception e) {
            throw new CustomChangeException(e);
        }
    }

    @Override
    public String getConfirmationMessage() {
        return null;
    }

    @Override
    public void setUp() throws SetupException {
    }

    @Override
    public void setFileOpener(ResourceAccessor resourceAccessor) {
    }

    @Override
    public ValidationErrors validate(Database database) {
        return null;
    }
}
