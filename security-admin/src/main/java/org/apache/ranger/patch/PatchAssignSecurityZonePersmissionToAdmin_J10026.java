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

import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.ranger.biz.XUserMgr;
import org.apache.ranger.common.RangerConstants;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXPortalUser;
import org.apache.ranger.service.XPortalUserService;
import org.apache.ranger.util.CLIUtil;
import org.apache.ranger.view.VXPortalUser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

@Component
public class PatchAssignSecurityZonePersmissionToAdmin_J10026 extends BaseLoader {

        private static final Logger logger = Logger
                        .getLogger(PatchAssignSecurityZonePersmissionToAdmin_J10026.class);

        @Autowired
        RangerDaoManager daoManager;

        @Autowired
        XUserMgr xUserMgr;

        @Autowired
        XPortalUserService xPortalUserService;

        public static void main(String[] args) {
                logger.info("main()");
                try {

                        PatchAssignSecurityZonePersmissionToAdmin_J10026 loader = (PatchAssignSecurityZonePersmissionToAdmin_J10026) CLIUtil
                                        .getBean(PatchAssignSecurityZonePersmissionToAdmin_J10026.class);

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
        public void init() throws Exception {
                // Do Nothing
        }

        @Override
        public void printStats() {
                // Do Nothing

        }

        @Override
        public void execLoad() {
                logger.info("==> PatchAssignSecurityZonePersmissionToAdmin_J10026.execLoad() started");
                assignSecurityZonePermissionToExistingAdminUsers();
                logger.info("<== PatchAssignSecurityZonePersmissionToAdmin_J10026.execLoad() completed");

        }

        private void assignSecurityZonePermissionToExistingAdminUsers(){
                int countUserPermissionUpdated = 0;
                List<XXPortalUser> xXPortalUsers =daoManager.getXXPortalUser().findByRole(RangerConstants.ROLE_SYS_ADMIN);
                if(xXPortalUsers != null && !CollectionUtils.isEmpty(xXPortalUsers)){
                        countUserPermissionUpdated=assignPermissions(xXPortalUsers);
                        logger.info("Security Zone Permission assigned to users having role:"+RangerConstants.ROLE_SYS_ADMIN+". Processed:"+countUserPermissionUpdated + " of total "+xXPortalUsers.size());
                }
        }

        private int assignPermissions(List<XXPortalUser> xXPortalUsers){
                HashMap<String, Long> moduleNameId = xUserMgr.getAllModuleNameAndIdMap();
                int countUserPermissionUpdated = 0;
                if(!CollectionUtils.isEmpty(xXPortalUsers)){
                        for (XXPortalUser xPortalUser : xXPortalUsers) {
                                try{
                                        if(xPortalUser!=null){
                                                VXPortalUser vPortalUser = xPortalUserService.populateViewBean(xPortalUser);
                                                if(vPortalUser!=null){
                                                        vPortalUser.setUserRoleList(daoManager.getXXPortalUserRole().findXPortalUserRolebyXPortalUserId(vPortalUser.getId()));
                                                        xUserMgr.createOrUpdateUserPermisson(vPortalUser, moduleNameId.get(RangerConstants.MODULE_SECURITY_ZONE), false);
                                                        countUserPermissionUpdated += 1;
                                                        logger.info("Security Zone Permission assigned/updated to Admin Role, UserId [" + xPortalUser.getId() + "]");
                                                }
                                        }
                                }catch(Exception ex){
                                        logger.error("Error while assigning security zone permission for admin users", ex);
                                        System.exit(1);
                                }
                        }
                }
                return countUserPermissionUpdated;
        }
}
