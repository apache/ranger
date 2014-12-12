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

import javax.annotation.Generated;
import javax.persistence.metamodel.SingularAttribute;
import javax.persistence.metamodel.StaticMetamodel;

@Generated(value="Dali", date="2014-02-02T14:05:13.483-0800")
@StaticMetamodel(XXHBaseAuditEvent.class)
public class XXHBaseAuditEvent_ extends XXBaseAuditEvent_ {
	public static volatile SingularAttribute<XXHBaseAuditEvent, String> resourcePath;
	public static volatile SingularAttribute<XXHBaseAuditEvent, String> resourceType;
	public static volatile SingularAttribute<XXHBaseAuditEvent, String> requestData;
}
