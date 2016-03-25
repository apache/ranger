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

package org.apache.ranger.plugin.util;

import java.io.Serializable;
import java.util.*;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility;
import org.codehaus.jackson.map.annotate.JsonSerialize;


@JsonAutoDetect(getterVisibility=Visibility.NONE, setterVisibility=Visibility.NONE, fieldVisibility=Visibility.ANY)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL )
@JsonIgnoreProperties(ignoreUnknown=true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
// Request to alter resources but not privileges
public class AlterRequest implements Serializable {
	private static final long serialVersionUID = 1L;

	private String              grantor                    = null;
	private Map<String, List<String>> oldResources         = null;
	private Map<String, List<String>> newResources         = null;
    private Boolean             delegateAdmin              = Boolean.FALSE;
	private Boolean             enableAudit                = Boolean.TRUE;
	private Boolean             replaceExistingPermissions = Boolean.FALSE;
    private Boolean             isRecursive                = Boolean.FALSE;
	private String              clientIPAddress            = null;
	private String              clientType                 = null;
	private String              requestData                = null;
	private String              sessionId                  = null;


	public AlterRequest() {
		this(null, null, null, null, null, null, null, null, null, null);
	}

	public AlterRequest(String grantor, Map<String, String> oldResource, Map<String, String> newResource,
                        Boolean delegateAdmin, Boolean enableAudit, Boolean replaceExistingPermissions,
                        Boolean isRecursive, String clientIPAddress, String clientType,
                        String requestData) {
		setGrantor(grantor);
		setOldResource(oldResource);
		setNewResource(newResource);
        setDelegateAdmin(delegateAdmin);
        setEnableAudit(enableAudit);
		setReplaceExistingPermissions(replaceExistingPermissions);
        setIsRecursive(isRecursive);
		setClientIPAddress(clientIPAddress);
		setClientType(clientType);
		setRequestData(requestData);
		setSessionId(sessionId);
	}

	/**
	 * @return the grantor
	 */
	public String getGrantor() {
		return grantor;
	}

	/**
	 * @param grantor the grantor to set
	 */
	public void setGrantor(String grantor) {
		this.grantor = grantor;
	}

	/**
	 * @return the resource
	 */
	public Map<String, List<String>> getOldResources() {
		return oldResources;
	}

	/**
	 * @param resource the resource to set
	 */
	public void setOldResource(Map<String, String> resource) {
        oldResources = new HashMap();
        if (resource != null) {
            Iterator<Map.Entry<String, String>> it = resource.entrySet().iterator();
            while (it.hasNext()) {
                List<String> values = new ArrayList();
                Map.Entry<String, String> entry = it.next();
                values.add(entry.getValue());
                oldResources.put(entry.getKey(), values);
            }
        }
	}

    public void setOldResources(Map<String, List<String>> resources) {
        this.oldResources = resources == null ? new HashMap<String, List<String>>() : resources;
    }

	/**
	 * @return the enableAudit
	 */
	public Boolean getEnableAudit() {
		return enableAudit;
	}

	/**
	 * @param enableAudit the enableAudit to set
	 */
	public void setEnableAudit(Boolean enableAudit) {
		this.enableAudit = enableAudit == null ? Boolean.TRUE : enableAudit;
	}

	/**
	 * @return the replaceExistingPermissions
	 */
	public Boolean getReplaceExistingPermissions() {
		return replaceExistingPermissions;
	}

	/**
	 * @param replaceExistingPermissions the replaceExistingPermissions to set
	 */
	public void setReplaceExistingPermissions(Boolean replaceExistingPermissions) {
		this.replaceExistingPermissions = replaceExistingPermissions == null ? Boolean.FALSE : replaceExistingPermissions;
	}

	/**
	 * @return the isRecursive
	 */
	public Boolean getIsRecursive() {
		return isRecursive;
	}

	/**
	 * @param isRecursive the isRecursive to set
	 */
	public void setIsRecursive(Boolean isRecursive) {
		this.isRecursive = isRecursive == null ? Boolean.FALSE : isRecursive;
	}

	/**
	 * @return the clientIPAddress
	 */
	public String getClientIPAddress() {
		return clientIPAddress;
	}

	/**
	 * @param clientIPAddress the clientIPAddress to set
	 */
	public void setClientIPAddress(String clientIPAddress) {
		this.clientIPAddress = clientIPAddress;
	}

	/**
	 * @return the clientType
	 */
	public String getClientType() {
		return clientType;
	}

	/**
	 * @param clientType the clientType to set
	 */
	public void setClientType(String clientType) {
		this.clientType = clientType;
	}

	/**
	 * @return the requestData
	 */
	public String getRequestData() {
		return requestData;
	}

	/**
	 * @param requestData the requestData to set
	 */
	public void setRequestData(String requestData) {
		this.requestData = requestData;
	}

	/**
	 * @return the sessionId
	 */
	public String getSessionId() {
		return sessionId;
	}

	/**
	 * @param sessionId the sessionId to set
	 */
	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}

    public Map<String, List<String>> getNewResources() {
        return newResources;
    }

    public void setNewResource(Map<String, String> resource) {
        newResources = new HashMap<>();
        if (resource != null) {
            Iterator<Map.Entry<String, String>> it = resource.entrySet().iterator();
            while (it.hasNext()) {
                List<String> values = new ArrayList();
                Map.Entry<String, String> entry = it.next();
                values.add(entry.getValue());
                newResources.put(entry.getKey(), values);
            }
        }
    }

    public void setNewResources(Map<String, List<String>> resources) {
        this.newResources = resources == null ? new HashMap<String, List<String>>() : resources;
    }

    public Boolean getDelegateAdmin() {
        return delegateAdmin;
    }

    public void setDelegateAdmin(Boolean delegateAdmin) {
        this.delegateAdmin = delegateAdmin;
    }

    @Override
	public String toString( ) {
		StringBuilder sb = new StringBuilder();

		toString(sb);

		return sb.toString();
	}

	public StringBuilder toString(StringBuilder sb) {
		sb.append("AlterRequest={");

		sb.append("grantor={").append(grantor).append("} ");

		sb.append("oldResource={");
		if(oldResources != null) {
			for(Map.Entry<String, List<String>> e : oldResources.entrySet()) {
				sb.append(e.getKey()).append("=").append(e.getValue()).append("; ");
			}
		}
		sb.append("} ");

        sb.append("newResource={");
        if(newResources != null) {
            for(Map.Entry<String, List<String>> e : newResources.entrySet()) {
                sb.append(e.getKey()).append("=").append(e.getValue()).append("; ");
            }
        }
        sb.append("} ");

		sb.append("delegateAdmin={").append(delegateAdmin).append("} ");
		sb.append("enableAudit={").append(enableAudit).append("} ");
		sb.append("replaceExistingPermissions={").append(replaceExistingPermissions).append("} ");
		sb.append("isRecursive={").append(isRecursive).append("} ");
		sb.append("clientIPAddress={").append(clientIPAddress).append("} ");
		sb.append("clientType={").append(clientType).append("} ");
		sb.append("requestData={").append(requestData).append("} ");
		sb.append("sessionId={").append(sessionId).append("} ");

		sb.append("}");

		return sb;
	}
}
