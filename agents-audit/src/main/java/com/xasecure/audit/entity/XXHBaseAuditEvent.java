package com.xasecure.audit.entity;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;

import com.xasecure.audit.model.EnumRepositoryType;
import com.xasecure.audit.model.HBaseAuditEvent;

/**
 * Entity implementation class for Entity: XAHBaseAuditEvent
 *
 */
@Entity
@DiscriminatorValue("1")
public class XXHBaseAuditEvent extends XXBaseAuditEvent implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private String resourcePath;
	private String resourceType;
	private String requestData;


	public XXHBaseAuditEvent() {
		super();
	}   

	public XXHBaseAuditEvent(HBaseAuditEvent event) {
		super(event);
		
		this.resourcePath = event.getResourcePath();
		this.resourceType = event.getResourceType();
		this.requestData  = event.getRequestData();
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

	@Column(name = "request_data")
	public String getRequestData() {
		return this.requestData;
	}

	public void setRequestData(String requestData) {
		this.requestData = requestData;
	}
   
}
