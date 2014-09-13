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

 package com.xasecure.audit.entity;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;
import javax.persistence.Table;

import com.xasecure.audit.model.HdfsAuditEvent;

/**
 * Entity implementation class for Entity: XXHdfsAuditEvent
 *
 */
@Entity
@DiscriminatorValue("2")
public class XXHdfsAuditEvent extends XXBaseAuditEvent implements Serializable {
	private static final long serialVersionUID = 1L;

	private String resourcePath;
	private String resourceType;


	public XXHdfsAuditEvent() {
		super();
	}

	public XXHdfsAuditEvent(HdfsAuditEvent event) {
		super(event);

		this.resourcePath = event.getResourcePath();
		this.resourceType = event.getResourceType();
	}

	@Column(name = "resource_path")
	public String getResourcePath() {
		return this.resourcePath;
	}

	public void setResourcePath(String resourcePath) {
		this.resourcePath = resourcePath;
	}   

	@Column(name = "resource_type")
	public String getResourceType() {
		return this.resourceType;
	}

	public void setResourceType(String resourceType) {
		this.resourceType = resourceType;
	}
   
}
