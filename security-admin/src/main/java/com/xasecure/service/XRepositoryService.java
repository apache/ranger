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

package com.xasecure.service;

import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.xasecure.common.AppConstants;
import com.xasecure.common.MessageEnums;
import com.xasecure.common.PropertiesUtil;
import com.xasecure.common.RESTErrorUtil;
import com.xasecure.common.SearchCriteria;
import com.xasecure.common.XACommonEnums;
import com.xasecure.view.VXAsset;
import com.xasecure.view.VXAssetList;
import com.xasecure.view.VXRepository;
import com.xasecure.view.VXRepositoryList;

@Service
public class XRepositoryService extends
		PublicAPIServiceBase<VXAsset, VXRepository> {

	@Autowired
	RESTErrorUtil restErrorUtil;

	String version;

	public XRepositoryService() {
		version = PropertiesUtil.getProperty("maven.project.version", "");
	}

	public VXRepository mapXAToPublicObject(VXAsset vXAsset) {
		VXRepository vRepo = new VXRepository();
		vRepo = super.mapBaseAttributesToPublicObject(vXAsset, vRepo);

		vRepo.setName(vXAsset.getName());
		vRepo.setDescription(vXAsset.getDescription());
		vRepo.setRepositoryType(AppConstants.getLabelFor_AssetType(vXAsset
				.getAssetType()));
		vRepo.setConfig(vXAsset.getConfig());
		
		int actStatus = vXAsset.getActiveStatus();
		boolean isAct = (actStatus == XACommonEnums.STATUS_DISABLED) ? false
				: true;

		vRepo.setIsActive(isAct);
		vRepo.setVersion(version);

		return vRepo;
	}

	public VXAsset mapPublicToXAObject(VXRepository vXRepo) {

		VXAsset vXAsset = new VXAsset();
		vXAsset = super.mapBaseAttributesToXAObject(vXRepo, vXAsset);

		vXAsset.setName(vXRepo.getName());
		vXAsset.setDescription(vXRepo.getDescription());
		vXAsset.setAssetType(AppConstants.getEnumFor_AssetType(vXRepo
				.getRepositoryType()));
		vXAsset.setConfig(vXRepo.getConfig());
		
		int actStatus = (!vXRepo.getIsActive()) ? XACommonEnums.STATUS_DISABLED
				: XACommonEnums.STATUS_ENABLED;

		vXAsset.setActiveStatus(actStatus);

		return vXAsset;
	}

	public SearchCriteria getMappedSearchParams(HttpServletRequest request,
			SearchCriteria searchCriteria) {

		Object typeObj = searchCriteria.getParamValue("type");
		Object statusObj = searchCriteria.getParamValue("status");

		ArrayList<Integer> statusList = new ArrayList<Integer>();
		if (statusObj == null) {
			statusList.add(XACommonEnums.STATUS_DISABLED);
			statusList.add(XACommonEnums.STATUS_ENABLED);
		} else {
			boolean status = restErrorUtil.parseBoolean(
					request.getParameter("status"), "Invalid value for "
							+ "status", MessageEnums.INVALID_INPUT_DATA, null,
					"status");
			int statusEnum = (status == false) ? AppConstants.STATUS_DISABLED
					: AppConstants.STATUS_ENABLED;
			statusList.add(statusEnum);
		}
		searchCriteria.addParam("status", statusList);

		if (typeObj != null) {
			String type = typeObj.toString();
			int typeEnum = AppConstants.getEnumFor_AssetType(type);
			searchCriteria.addParam("type", typeEnum);
		}
		return searchCriteria;
	}

	public VXRepositoryList mapToVXRepositoryList(VXAssetList vXAssetList) {

		List<VXRepository> repoList = new ArrayList<VXRepository>();
		for (VXAsset vXAsset : vXAssetList.getVXAssets()) {
			VXRepository vXRepo = mapXAToPublicObject(vXAsset);
			repoList.add(vXRepo);
		}
		VXRepositoryList vXRepositoryList = new VXRepositoryList(repoList);
		return vXRepositoryList;
	}

}
