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

package org.apache.ranger.patch;

import org.apache.log4j.Logger;
import org.apache.ranger.biz.ServiceDBStore;
import org.apache.ranger.common.GUIDUtil;
import org.apache.ranger.common.JSONUtil;
import org.apache.ranger.common.RangerValidatorFactory;
import org.apache.ranger.common.StringUtil;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.validation.RangerServiceDefValidator;
import org.apache.ranger.plugin.model.validation.RangerValidator;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.apache.ranger.service.RangerPolicyService;
import org.apache.ranger.util.CLIUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Component
public class PatchForTrinoToSupportTrino333_J10038 extends BaseLoader {
  private static final Logger logger = Logger.getLogger(PatchForTrinoToSupportTrino333_J10038.class);

  private static final List<String> TRINO_RESOURCES = new ArrayList<>(
    Arrays.asList("function", "procedure", "trinouser", "systemproperty", "sessionproperty"));

  private static final List<String> TRINO_ACCESS_TYPES = new ArrayList<>(
    Arrays.asList("grant", "revoke", "show", "impersonate", "execute", "delete"));

  @Autowired
  RangerDaoManager daoMgr;

  @Autowired
  ServiceDBStore svcDBStore;

  @Autowired
  GUIDUtil guidUtil;

  @Autowired
  JSONUtil jsonUtil;

  @Autowired
  StringUtil stringUtil;

  @Autowired
  RangerValidatorFactory validatorFactory;

  @Autowired
  ServiceDBStore svcStore;

  @Autowired
  RangerPolicyService policyService;

  public static void main(String[] args) {
    logger.info("main()");
    try {
      PatchForTrinoToSupportTrino333_J10038 loader = (PatchForTrinoToSupportTrino333_J10038) CLIUtil
        .getBean(PatchForTrinoToSupportTrino333_J10038.class);
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
  public void execLoad() {
    logger.info("==> PatchForTrinoToSupportTrino333.execLoad()");
    try {
      addTrino333Support();
    } catch (Exception e) {
      throw new RuntimeException(
        "Error while updating " + EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_TRINO_NAME + " service-def");
    }
    logger.info("<== PatchForTrinoToSupportTrino333.execLoad()");
  }

  @Override
  public void printStats() {
    logger.info("PatchForTrinoToSupportTrino333 Logs");
  }

  private void addTrino333Support() throws Exception {
    RangerServiceDef ret = null;
    RangerServiceDef embeddedTrinoServiceDef = null;
    XXServiceDef xXServiceDefObj = null;
    RangerServiceDef dbTrinoServiceDef = null;
    List<RangerServiceDef.RangerResourceDef> embeddedTrinoResourceDefs = null;
    List<RangerServiceDef.RangerAccessTypeDef> embeddedTrinoAccessTypes = null;

    embeddedTrinoServiceDef = EmbeddedServiceDefsUtil.instance()
      .getEmbeddedServiceDef(EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_TRINO_NAME);

    if (embeddedTrinoServiceDef != null) {
      xXServiceDefObj = daoMgr.getXXServiceDef()
        .findByName(EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_TRINO_NAME);
      if (xXServiceDefObj == null) {
        logger.info(xXServiceDefObj + ": service-def not found. No patching is needed");
        return;
      }

      dbTrinoServiceDef = svcDBStore.getServiceDefByName(EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_TRINO_NAME);

      embeddedTrinoResourceDefs = embeddedTrinoServiceDef.getResources();
      embeddedTrinoAccessTypes = embeddedTrinoServiceDef.getAccessTypes();
      if (checkResourcePresent(TRINO_RESOURCES, embeddedTrinoResourceDefs)) {
        dbTrinoServiceDef.setResources(embeddedTrinoResourceDefs);
        if (checkAccessPresent(TRINO_ACCESS_TYPES, embeddedTrinoAccessTypes)) {
          dbTrinoServiceDef.setAccessTypes(embeddedTrinoAccessTypes);
        }
      }

      RangerServiceDefValidator validator = validatorFactory.getServiceDefValidator(svcStore);
      validator.validate(dbTrinoServiceDef, RangerValidator.Action.UPDATE);
      ret = svcStore.updateServiceDef(dbTrinoServiceDef);
      if (ret == null) {
        logger.error("Error while updating " + EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_ATLAS_NAME
          + " service-def");
        throw new RuntimeException("Error while updating "
          + EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_ATLAS_NAME + " service-def");
      }

    }
  }

  private boolean checkResourcePresent(List<String> resources, List<RangerServiceDef.RangerResourceDef> resourceDefs) {
    boolean ret = false;
    for (RangerServiceDef.RangerResourceDef resourceDef : resourceDefs) {
      if (resources.contains(resourceDef.getName())) {
        ret = true;
        break;
      }
    }
    return ret;
  }

  private boolean checkAccessPresent(List<String> accesses, List<RangerServiceDef.RangerAccessTypeDef> embeddedAtlasAccessTypes) {
    boolean ret = false;
    for (RangerServiceDef.RangerAccessTypeDef accessDef : embeddedAtlasAccessTypes) {
      if (accesses.contains(accessDef.getName())) {
        ret = true;
        break;
      }
    }
    return ret;
  }
}
