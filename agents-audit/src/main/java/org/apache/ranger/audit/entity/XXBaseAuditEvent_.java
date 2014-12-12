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

 package org.apache.ranger.audit.entity;

import java.util.Date;

import javax.annotation.Generated;
import javax.persistence.metamodel.SingularAttribute;
import javax.persistence.metamodel.StaticMetamodel;

import org.apache.ranger.audit.model.EnumRepositoryType;

@Generated(value="Dali", date="2014-02-04T07:25:42.940-0800")
@StaticMetamodel(XXBaseAuditEvent.class)
public class XXBaseAuditEvent_ {
	public static volatile SingularAttribute<XXBaseAuditEvent, Long> auditId;
	public static volatile SingularAttribute<XXBaseAuditEvent, String> agentId;
	public static volatile SingularAttribute<XXBaseAuditEvent, String> user;
	public static volatile SingularAttribute<XXBaseAuditEvent, Date> timeStamp;
	public static volatile SingularAttribute<XXBaseAuditEvent, Long> policyId;
	public static volatile SingularAttribute<XXBaseAuditEvent, String> accessType;
	public static volatile SingularAttribute<XXBaseAuditEvent, Short> accessResult;
	public static volatile SingularAttribute<XXBaseAuditEvent, String> resultReason;
	public static volatile SingularAttribute<XXBaseAuditEvent, String> aclEnforcer;
	public static volatile SingularAttribute<XXBaseAuditEvent, EnumRepositoryType> repositoryType;
	public static volatile SingularAttribute<XXBaseAuditEvent, String> repositoryName;
	public static volatile SingularAttribute<XXBaseAuditEvent, String> sessionId;
	public static volatile SingularAttribute<XXBaseAuditEvent, String> clientType;
	public static volatile SingularAttribute<XXBaseAuditEvent, String> clientIP;
	public static volatile SingularAttribute<XXBaseAuditEvent, String> action;
}
