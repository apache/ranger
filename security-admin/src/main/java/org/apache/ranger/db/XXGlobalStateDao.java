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

package org.apache.ranger.db;

import com.google.gson.Gson;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.ranger.common.DateUtil;
import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.entity.XXGlobalState;
import org.springframework.stereotype.Service;

import javax.persistence.NoResultException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Service
public class XXGlobalStateDao extends BaseDao<XXGlobalState> {
    private static final Logger logger = Logger.getLogger(RangerDaoManager.class);

    final static String APP_DATA_ENTRY_ROLE_VERSION = "RangerRoleVersion";

    public void onGlobalStateChange(String stateName) throws Exception {

        if (StringUtils.isBlank(stateName)) {
            logger.error("Invalid name for state:[" + stateName +"]");
            throw new Exception("Invalid name for state:[" + stateName +"]");
        } else {
            try {
                XXGlobalState globalState = findByStateName(stateName);
                if (globalState == null) {
                    globalState = new XXGlobalState();
                    globalState.setStateName(stateName);
                    create(globalState);
                } else {
                    Date date = DateUtil.getUTCDate();
                    if (date == null) {
                        date = new Date();
                    }
                    globalState.setAppData(date.toString());

                    update(globalState);
                }
            } catch (Exception exception) {
                logger.error("Cannot create/update GlobalState for state:[" + stateName + "]", exception);
                throw exception;
            }
        }
    }

    public void onGlobalAppDataChange(String stateName) throws Exception {

        if (StringUtils.isBlank(stateName)) {
            logger.error("Invalid name for state:[" + stateName +"]");
            throw new Exception("Invalid name for state:[" + stateName +"]");
        } else {
            try {
                XXGlobalState globalState = findByStateName(stateName);
                if (globalState == null) {
                    createGlobalStateForRoleVersion(globalState, stateName);
                } else {
                    updateGlobalStateForRoleVersion(globalState, stateName);
                }
            } catch (Exception exception) {
                logger.error("Cannot create/update GlobalState for state:[" + stateName + "]", exception);
                throw exception;
            }
        }
    }

    public Long getRoleVersion(String stateName) {
        Long ret = null;
        try {
            XXGlobalState       globalState     = findByStateName(stateName);
            Map<String, String> roleVersionJson = new Gson().fromJson(globalState.getAppData(), Map.class);
            if(MapUtils.isNotEmpty(roleVersionJson)) {
                ret = Long.valueOf(roleVersionJson.get(APP_DATA_ENTRY_ROLE_VERSION));
            } else {
                ret = 1L;
            }
        } catch (Exception exception) {
            logger.warn("Unable to find the role version in Ranger Database");
        }
        return ret;
    }

    /**
     * Default Constructor
     */
    public XXGlobalStateDao(RangerDaoManagerBase daoManager) {
        super(daoManager);
    }
    public XXGlobalState findByStateId(Long stateId) {
        if (stateId == null) {
            return null;
        }
        try {
            XXGlobalState xxGlobalState = getEntityManager()
                    .createNamedQuery("XXGlobalState.findByStateId", tClass)
                    .setParameter("stateId", stateId)
                    .getSingleResult();
            return xxGlobalState;
        } catch (NoResultException e) {
            return null;
        }
    }
    public XXGlobalState findByStateName(String stateName) {
        if (StringUtils.isBlank(stateName)) {
            return null;
        }
        try {
            XXGlobalState xxGlobalState = getEntityManager()
                    .createNamedQuery("XXGlobalState.findByStateName", tClass)
                    .setParameter("stateName", stateName)
                    .getSingleResult();
            return xxGlobalState;
        } catch (NoResultException e) {
            return null;
        }
    }

    private void createGlobalStateForRoleVersion(XXGlobalState globalState, String stateName) {
        globalState.setStateName(stateName);
        Map<String,String> roleVersion = new HashMap<>();
        roleVersion.put(APP_DATA_ENTRY_ROLE_VERSION,new String(Long.toString(1L)));
        globalState.setAppData(new Gson().toJson(roleVersion));
        create(globalState);
    }

    private void updateGlobalStateForRoleVersion(XXGlobalState globalState, String stateName) {
        Map<String,String> roleVersionJson = new Gson().fromJson(globalState.getAppData(),Map.class);
        if (MapUtils.isNotEmpty(roleVersionJson)) {
            Long roleVersion = Long.valueOf(roleVersionJson.get(APP_DATA_ENTRY_ROLE_VERSION)) + 1L;
            roleVersionJson.put(APP_DATA_ENTRY_ROLE_VERSION, new String(Long.toString(roleVersion)));
            globalState.setAppData(new Gson().toJson(roleVersionJson));
            update(globalState);
        } else {
            //if not present create Global State for Role Version.
            XXGlobalState xxGlobalState = new XXGlobalState();
            createGlobalStateForRoleVersion(xxGlobalState, stateName);
        }
    }
}

