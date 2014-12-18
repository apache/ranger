package org.apache.ranger.plugin.policyengine;

import java.util.Collection;


public class RangerAccessRequestImpl implements RangerAccessRequest {
	private RangerResource     resource          = null;
	private Collection<String> accessTypes       = null;
	private String             requestUser       = null;
	private Collection<String> requestUserGroups = null;
	private String             clientIPAddress   = null;
	private String             clientType        = null;
	private String             action            = null;
	private String             requestData       = null;
	private String             sessionId         = null;

	@Override
	public RangerResource getResource() {
		return resource;
	}

	@Override
	public Collection<String> getAccessTypes() {
		return accessTypes;
	}

	@Override
	public String getRequestUser() {
		return requestUser;
	}

	@Override
	public Collection<String> getRequestUserGroups() {
		return requestUserGroups;
	}

	@Override
	public String getClientIPAddress() {
		return clientIPAddress;
	}

	@Override
	public String getClientType() {
		return clientType;
	}

	@Override
	public String getAction() {
		return action;
	}

	@Override
	public String getRequestData() {
		return requestData;
	}

	@Override
	public String getSessionId() {
		return sessionId;
	}


	public void setResource(RangerResource resource) {
		this.resource = resource;
	}

	public void setAccessTypes(Collection<String> accessTypes) {
		this.accessTypes = accessTypes;
	}

	public void setRequestUser(String requestUser) {
		this.requestUser = requestUser;
	}

	public void setRequestUserGroups(Collection<String> requestUserGroups) {
		this.requestUserGroups = requestUserGroups;
	}

	public void setClientIPAddress(String clientIPAddress) {
		this.clientIPAddress = clientIPAddress;
	}

	public void setClientType(String clientType) {
		this.clientType = clientType;
	}

	public void setAction(String action) {
		this.action = action;
	}

	public void setRequestData(String requestData) {
		this.requestData = requestData;
	}

	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}
}
