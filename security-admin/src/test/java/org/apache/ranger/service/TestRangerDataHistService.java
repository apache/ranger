/* Copyright 2004, 2005, 2006 Acegi Technology Pty Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import java.util.Date;

import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXDataHist;
import org.apache.ranger.plugin.model.RangerBaseModelObject;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestRangerDataHistService {

	@InjectMocks
	RangerDataHistService rangerDataHistService;

	@Mock
	RangerBaseModelObject baseModelObj;

	@Mock
	RangerDaoManager daoMgr;
	@Mock
	org.apache.ranger.db.XXDataHistDao XXDataHistDao;

	@Mock
	ObjectMapper mapper;

	@Test
	public void test1CreateObjectDataHistory() {
		String action = "create";
		RangerBaseModelObject baseModelObj = new RangerBaseModelObject();
		Mockito.when(daoMgr.getXXDataHist()).thenReturn(XXDataHistDao);
		rangerDataHistService.createObjectDataHistory(baseModelObj, action);

	}

	@Test
	public void test2WriteObjectAsString() {
		RangerBaseModelObject testbaseModelObj = createRangerBaseModelObject();
		rangerDataHistService.writeObjectAsString(testbaseModelObj);

	}

	@Test
	public void test3WriteJsonToJavaObject() {
		XXDataHist XXDataHist = createXXDataHistObject();
		rangerDataHistService.writeJsonToJavaObject(XXDataHist.getContent(), RangerPolicy.class);
	}

	private XXDataHist createXXDataHistObject() {
		XXDataHist xDataHist = new XXDataHist();
		Date date = new Date();
		xDataHist.setObjectId(1L);
		xDataHist.setObjectGuid("testGuid");
		xDataHist.setCreateTime(date);
		xDataHist.setAction("Create");
		xDataHist.setVersion(1L);
		xDataHist.setUpdateTime(date);
		xDataHist.setFromTime(date);

		xDataHist.setContent("{\"id\":3,\"guid\":\"c3991965-b063-4fff-b9e5-2e0aa566af29\""
				+ ",\"isEnabled\":true,\"createdBy\":\"Admin\",\""
				+ "updatedBy\":\"Admin\",\"createTime\":1529044511083,\"updateTime\""
				+ ":1529044511084,\"version\":1,\"service\":\"cl1_storm_01\",\"name\""
				+ ":\"all - topology\",\"policyType\":0,\"description\":\"Policy for all - topology\""
				+ ",\"resourceSignature\":\"61c9e34a1c273ed9263940cf79275e94\",\"isAuditEnabled\""
				+ ":true,\"resources\":{\"topology\":{\"values\":[\"*\"],\"isExcludes\":false,\"isRecursive\""
				+ ":false}},\"policyItems\":[{\"accesses\":[{\"type\":\"submitTopology\",\"isAllowed\":true}"
				+ ",{\"type\":\"fileUpload\",\"isAllowed\":true},{\"type\":\"fileDownload\",\"isAllowed\""
				+ ":true},{\"type\":\"killTopology\",\"isAllowed\":true},{\"type\":\"rebalance\",\"isAllowed\""
				+ ":true},{\"type\":\"activate\",\"isAllowed\":true},{\"type\":\"deactivate\",\"isAllowed\":true},{\"type\":\"getTopologyConf\",\"isAllowed\":true},{\"type\":\"getTopology\",\"isAllowed\":true},{\"type\":\"getUserTopology\",\"isAllowed\":true},{\"type\":\"getTopologyInfo\",\"isAllowed\":true},{\"type\":\"uploadNewCredentials\",\"isAllowed\":true}],\"users\":[\"test\",\"test123\",\"HTTP123\"],\"groups\":[],\"conditions\":[],\"delegateAdmin\":true}],\"denyPolicyItems\":[],\"allowExceptions\":[],\"denyExceptions\":[],\"dataMaskPolicyItems\":[],\"rowFilterPolicyItems\":[]} ");

		return xDataHist;
	}

	private RangerBaseModelObject createRangerBaseModelObject() {
		RangerBaseModelObject baseModelObj = new RangerBaseModelObject();
		Date date = new Date();
		baseModelObj.setCreatedBy("admin");
		baseModelObj.setCreateTime(date);
		baseModelObj.setGuid("testGuid");
		baseModelObj.setIsEnabled(true);
		baseModelObj.setUpdatedBy("admin");
		baseModelObj.setUpdateTime(date);
		baseModelObj.setVersion(1L);
		return baseModelObj;

	}
}
