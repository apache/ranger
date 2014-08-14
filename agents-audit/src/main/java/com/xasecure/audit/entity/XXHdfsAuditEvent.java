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
