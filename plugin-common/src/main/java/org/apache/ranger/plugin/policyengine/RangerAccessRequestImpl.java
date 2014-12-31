package org.apache.ranger.plugin.policyengine;

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;


public class RangerAccessRequestImpl implements RangerAccessRequest {
	private RangerResource      resource        = null;
	private String              accessType      = null;
	private String              user            = null;
	private Collection<String>  userGroups      = null;
	private Date                accessTime      = null;
	private String              clientIPAddress = null;
	private String              clientType      = null;
	private String              action          = null;
	private String              requestData     = null;
	private String              sessionId       = null;
	private Map<String, Object> context         = null;


	public RangerAccessRequestImpl() {
		this(null, null, null, null);
	}

	public RangerAccessRequestImpl(RangerResource resource, String accessType, String user, Collection<String> userGroups) {
		setResource(resource);
		setAccessType(accessType);
		setUser(user);
		setUserGroups(userGroups);

		// set remaining fields to default value
		setAccessTime(null);
		setClientIPAddress(null);
		setClientType(null);
		setAction(null);
		setRequestData(null);
		setSessionId(null);
		setContext(null);
	}

	@Override
	public RangerResource getResource() {
		return resource;
	}

	@Override
	public String getAccessType() {
		return accessType;
	}

	@Override
	public String getUser() {
		return user;
	}

	@Override
	public Collection<String> getUserGroups() {
		return userGroups;
	}

	@Override
	public Date getAccessTime() {
		return accessTime;
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

	@Override
	public Map<String, Object> getContext() {
		return context;
	}


	public void setResource(RangerResource resource) {
		this.resource = resource;
	}

	public void setAccessType(String accessType) {
		this.accessType = accessType;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public void setUserGroups(Collection<String> userGroups) {
		this.userGroups = (userGroups == null) ? new HashSet<String>() : userGroups;
	}

	public void setAccessTime(Date accessTime) {
		this.accessTime = (accessTime == null) ? new Date() : accessTime;
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

	public void setContext(Map<String, Object> context) {
		this.context = (context == null) ? new HashMap<String, Object>() : context;
	}

	@Override
	public String toString( ) {
		StringBuilder sb = new StringBuilder();

		toString(sb);

		return sb.toString();
	}

	public StringBuilder toString(StringBuilder sb) {
		sb.append("RangerAccessRequestImpl={");

		sb.append("resource={").append(resource).append("} ");
		sb.append("accessType={").append(accessType).append("} ");
		sb.append("user={").append(user).append("} ");

		sb.append("userGroups={");
		if(userGroups != null) {
			for(String userGroup : userGroups) {
				sb.append(userGroup).append(" ");
			}
		}

		sb.append("accessTime={").append(accessTime).append("} ");
		sb.append("clientIPAddress={").append(clientIPAddress).append("} ");
		sb.append("clientType={").append(clientType).append("} ");
		sb.append("action={").append(action).append("} ");
		sb.append("requestData={").append(requestData).append("} ");
		sb.append("sessionId={").append(sessionId).append("} ");


		sb.append("context={");
		if(context != null) {
			for(Map.Entry<String, Object> e : context.entrySet()) {
				sb.append(e.getKey()).append("={").append(e.getValue()).append("} ");
			}
		}
		sb.append("} ");

		sb.append("}");

		return sb;
	}
}
