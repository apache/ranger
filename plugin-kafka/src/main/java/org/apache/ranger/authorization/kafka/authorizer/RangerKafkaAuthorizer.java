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

package org.apache.ranger.authorization.kafka.authorizer;

import java.util.Date;

import kafka.security.auth.Acl;
import kafka.security.auth.Authorizer;
import kafka.security.auth.KafkaPrincipal;
import kafka.security.auth.Operation;
import kafka.security.auth.Resource;
import kafka.security.auth.ResourceType;
import kafka.server.KafkaConfig;
import kafka.network.RequestChannel.Session;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.authorization.utils.StringUtil;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;

import scala.collection.immutable.HashSet;
import scala.collection.immutable.Set;

public class RangerKafkaAuthorizer implements Authorizer {
	private static final Log logger = LogFactory
			.getLog(RangerKafkaAuthorizer.class);

	public static final String KEY_TOPIC = "topic";
	public static final String KEY_CLUSTER = "cluster";
	public static final String KEY_CONSUMER_GROUP = "consumer_group";

	public static final String ACCESS_TYPE_READ = "read";
	public static final String ACCESS_TYPE_WRITE = "write";
	public static final String ACCESS_TYPE_CREATE = "create";
	public static final String ACCESS_TYPE_DELETE = "delete";
	public static final String ACCESS_TYPE_ALTER = "alter";
	public static final String ACCESS_TYPE_DESCRIBE = "describe";
	public static final String ACCESS_TYPE_KAFKA_ADMIN = "kafka_admin";

	private static volatile RangerBasePlugin rangerPlugin = null;

	public RangerKafkaAuthorizer() {
		if (rangerPlugin == null) {
			rangerPlugin = new RangerBasePlugin("kafka", "kafka");
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see kafka.security.auth.Authorizer#initialize(kafka.server.KafkaConfig)
	 */
	@Override
	public void initialize(KafkaConfig kafkaConfig) {
		rangerPlugin.init();
		RangerDefaultAuditHandler auditHandler = new RangerDefaultAuditHandler();

		rangerPlugin.setResultProcessor(auditHandler);
	}

	// TODO: Fix this after Session is fixed
	// @Override
	public boolean authorize(Session session, Operation operation,
			Resource resource) {

		String userName = null;
		java.util.Set<String> userGroups = getGroupsForUser(userName);
		String ip = null;
		Date eventTime = StringUtil.getUTCDate();
		String accessType = mapToRangerAccessType(operation);
		if (accessType == null) {
			logger.fatal("Unsupported access type. session=" + session
					+ ", operation=" + operation + ", resource=" + resource);
			return false;
		}
		String action = accessType;

		RangerAccessRequestImpl rangerRequest = new RangerAccessRequestImpl();
		rangerRequest.setUser(userName);
		rangerRequest.setUserGroups(userGroups);
		rangerRequest.setClientIPAddress(ip);
		rangerRequest.setAccessTime(eventTime);

		RangerAccessResourceImpl rangerResource = new RangerAccessResourceImpl();

		if (resource.resourceType().equals(ResourceType.TOPIC)) {
			rangerResource.setValue(KEY_TOPIC, resource.name());
		} else if (resource.resourceType().equals(ResourceType.CLUSTER)) {
			rangerResource.setValue(KEY_CLUSTER, resource.name());
		} else if (resource.resourceType().equals(ResourceType.CONSUMER_GROUP)) {
			rangerResource.setValue(KEY_CONSUMER_GROUP, resource.name());
		} else {
			logger.fatal("Unsupported resourceType=" + resource.resourceType());
			return false;
		}

		rangerRequest.setResource(rangerResource);
		rangerRequest.setAccessType(accessType);
		rangerRequest.setAction(action);
		rangerRequest.setRequestData(resource.name());

		RangerAccessResult result = rangerPlugin.isAccessAllowed(rangerRequest);
		return result.getIsAllowed();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * kafka.security.auth.Authorizer#addAcls(scala.collection.immutable.Set,
	 * kafka.security.auth.Resource)
	 */
	@Override
	public void addAcls(Set<Acl> acls, Resource resource) {
		logger.error("addAcls() is not supported by Ranger for Kafka");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * kafka.security.auth.Authorizer#removeAcls(scala.collection.immutable.Set,
	 * kafka.security.auth.Resource)
	 */
	@Override
	public boolean removeAcls(Set<Acl> acls, Resource resource) {
		logger.error("removeAcls() is not supported by Ranger for Kafka");
		return false;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * kafka.security.auth.Authorizer#removeAcls(kafka.security.auth.Resource)
	 */
	@Override
	public boolean removeAcls(Resource resource) {
		logger.error("removeAcls() is not supported by Ranger for Kafka");
		return false;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see kafka.security.auth.Authorizer#getAcls(kafka.security.auth.Resource)
	 */
	@Override
	public Set<Acl> getAcls(Resource resource) {
		Set<Acl> aclList = new HashSet<Acl>();
		logger.error("getAcls() is not supported by Ranger for Kafka");

		return aclList;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * kafka.security.auth.Authorizer#getAcls(kafka.security.auth.KafkaPrincipal
	 * )
	 */
	@Override
	public Set<Acl> getAcls(KafkaPrincipal principal) {
		Set<Acl> aclList = new HashSet<Acl>();
		logger.error("getAcls() is not supported by Ranger for Kafka");
		return aclList;
	}

	/**
	 * @param userName
	 * @return
	 */
	private java.util.Set<String> getGroupsForUser(String userName) {
		if (userName == null) {
			return null;
		}

		// TODO: Need to implement this method
		return null;
	}

	/**
	 * @param operation
	 * @return
	 */
	private String mapToRangerAccessType(Operation operation) {
		if (operation.equals(Operation.READ)) {
			return ACCESS_TYPE_READ;
		} else if (operation.equals(Operation.WRITE)) {
			return ACCESS_TYPE_WRITE;
		} else if (operation.equals(Operation.CREATE)) {
			return ACCESS_TYPE_CREATE;
		} else if (operation.equals(Operation.DELETE)) {
			return ACCESS_TYPE_DELETE;
		} else if (operation.equals(Operation.ALTER)) {
			return ACCESS_TYPE_ALTER;
		} else if (operation.equals(Operation.DESCRIBE)) {
			return ACCESS_TYPE_DESCRIBE;
		} else if (operation.equals(Operation.CLUSTER_ACTION)) {
			return ACCESS_TYPE_KAFKA_ADMIN;
		}
		return null;
	}
}
