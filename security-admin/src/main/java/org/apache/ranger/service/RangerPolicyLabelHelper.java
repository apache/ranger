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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.biz.ServiceDBStore;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXPolicy;
import org.apache.ranger.entity.XXPolicyLabel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Service
@Scope("singleton")
public class RangerPolicyLabelHelper {
	private static final Log LOG = LogFactory.getLog(ServiceDBStore.class);

	@Autowired
	protected RangerDaoManager daoMgr;

	@Autowired
	RangerAuditFields<?> rangerAuditFields;

	@Transactional(propagation = Propagation.REQUIRES_NEW)
	public XXPolicyLabel createNewOrGetLabel(String policyLabel, XXPolicy xPolicy) {

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyLabelHelper.createNewOrGetLabel()");
		}

		XXPolicyLabel xxPolicyLabel = daoMgr.getXXPolicyLabels().findByName(policyLabel);
		if (xxPolicyLabel == null) {
			xxPolicyLabel = new XXPolicyLabel();
			if (StringUtils.isNotEmpty(policyLabel)) {
				xxPolicyLabel.setPolicyLabel(policyLabel);
				xxPolicyLabel = rangerAuditFields.populateAuditFieldsForCreate(xxPolicyLabel);
				xxPolicyLabel = daoMgr.getXXPolicyLabels().create(xxPolicyLabel);
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyLabelHelper.createNewOrGetLabel(), xxPolicyLabel = " + xxPolicyLabel);
		}

		return xxPolicyLabel;
	}

}
