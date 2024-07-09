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

package org.apache.ranger.service;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.ranger.biz.ServiceDBStore;
import org.apache.ranger.db.XXServiceVersionInfoDao;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.entity.XXServiceConfigMap;
import org.apache.ranger.entity.XXServiceVersionInfo;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.util.PasswordUtils;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;


@Service
@Scope("singleton")
public class RangerServiceService extends RangerServiceServiceBase<XXService, RangerService> {
	String actionCreate;
	String actionUpdate;
	String actionDelete;

	public RangerServiceService() {
		super();

		actionCreate = "create";
		actionUpdate = "update";
		actionDelete = "delete";
	}

	@Override
	protected XXService mapViewToEntityBean(RangerService vObj, XXService xObj, int OPERATION_CONTEXT) {
		return super.mapViewToEntityBean(vObj, xObj, OPERATION_CONTEXT);
	}

	@Override
	protected RangerService mapEntityToViewBean(RangerService vObj, XXService xObj) {
		return super.mapEntityToViewBean(vObj, xObj);
	}
	
	@Override
	protected void validateForCreate(RangerService vObj) {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected void validateForUpdate(RangerService vService, XXService xService) {
		
	}
	
	@Override
	protected RangerService populateViewBean(XXService xService) {
		RangerService vService = super.populateViewBean(xService);
		
		HashMap<String, String> configs = new HashMap<String, String>();
		List<XXServiceConfigMap> svcConfigMapList = daoMgr.getXXServiceConfigMap()
				.findByServiceId(xService.getId());
		for(XXServiceConfigMap svcConfMap : svcConfigMapList) {
			String configValue = svcConfMap.getConfigvalue();
			
			if(StringUtils.equalsIgnoreCase(svcConfMap.getConfigkey(), ServiceDBStore.CONFIG_KEY_PASSWORD)) {
				configValue = ServiceDBStore.HIDDEN_PASSWORD_STR;
			}
			configs.put(svcConfMap.getConfigkey(), configValue);
		}
		vService.setConfigs(configs);
		return vService;
	}

	public RangerService getPopulatedViewObject(XXService xService) {
		return this.populateViewBean(xService);
	}
	
	public List<RangerService> getAllServices() {
		List<XXService> xxServiceList = daoMgr.getXXService().getAll();
		List<RangerService> serviceList = new ArrayList<RangerService>();
		
		for(XXService xxService : xxServiceList) {
			RangerService service = populateViewBean(xxService);
			serviceList.add(service);
		}
		return serviceList;
	}

	public Map<String, String> getConfigsWithDecryptedPassword(RangerService service) throws Exception  {
		Map<String, String> configs = service.getConfigs();
		
		String pwd = configs.get(ServiceDBStore.CONFIG_KEY_PASSWORD);
		if(!stringUtil.isEmpty(pwd) && ServiceDBStore.HIDDEN_PASSWORD_STR.equalsIgnoreCase(pwd)) {
			XXServiceConfigMap pwdConfig = daoMgr.getXXServiceConfigMap().findByServiceAndConfigKey(service.getId(),
					ServiceDBStore.CONFIG_KEY_PASSWORD);

			if (pwdConfig != null) {
				String encryptedPwd = pwdConfig.getConfigvalue();
				if (encryptedPwd.contains(",")) {
					PasswordUtils util = PasswordUtils.build(encryptedPwd);
					String freeTextPasswordMetaData = Joiner.on(",").skipNulls().join(util.getCryptAlgo(),
							new String(util.getEncryptKey()), new String(util.getSalt()), util.getIterationCount(),
							PasswordUtils.needsIv(util.getCryptAlgo()) ? util.getIvAsString() : null);
					String decryptedPwd = PasswordUtils.decryptPassword(encryptedPwd);
					if (StringUtils
							.equalsIgnoreCase(
									freeTextPasswordMetaData + ","
											+ PasswordUtils
													.encryptPassword(freeTextPasswordMetaData + "," + decryptedPwd),
									encryptedPwd)) {
						configs.put(ServiceDBStore.CONFIG_KEY_PASSWORD, encryptedPwd); // XXX: method name is
																						// getConfigsWithDecryptedPassword,
																						// then why do we store the
																						// encryptedPwd?
					}
				} else {
					String decryptedPwd = PasswordUtils.decryptPassword(encryptedPwd);
					if (StringUtils.equalsIgnoreCase(PasswordUtils.encryptPassword(decryptedPwd), encryptedPwd)) {
						configs.put(ServiceDBStore.CONFIG_KEY_PASSWORD, encryptedPwd); // XXX: method name is
																						// getConfigsWithDecryptedPassword,
																						// then why do we store the
																						// encryptedPwd?
					}
				}
			}
		}
		return configs;
	}

	@Override
	public RangerService postCreate(XXService xObj) {
		XXServiceVersionInfo serviceVersionInfo = new XXServiceVersionInfo();

		serviceVersionInfo.setServiceId(xObj.getId());
		serviceVersionInfo.setPolicyVersion(1L);
		serviceVersionInfo.setTagVersion(1L);
		serviceVersionInfo.setRoleVersion(1L);
		Date now = new Date();
		serviceVersionInfo.setPolicyUpdateTime(now);
		serviceVersionInfo.setTagUpdateTime(now);
		serviceVersionInfo.setRoleUpdateTime(now);

		XXServiceVersionInfoDao serviceVersionInfoDao = daoMgr.getXXServiceVersionInfo();

		XXServiceVersionInfo createdServiceVersionInfo = serviceVersionInfoDao.create(serviceVersionInfo);

		return createdServiceVersionInfo != null ? super.postCreate(xObj) : null;
	}

	@Override
	protected XXService preDelete(Long id) {
		XXService ret = super.preDelete(id);

		if (ret != null) {
			XXServiceVersionInfoDao serviceVersionInfoDao = daoMgr.getXXServiceVersionInfo();

			XXServiceVersionInfo serviceVersionInfo = serviceVersionInfoDao.findByServiceId(id);

			if (serviceVersionInfo != null) {
				serviceVersionInfoDao.remove(serviceVersionInfo.getId());
			}
		}
		return ret;
	}
}
