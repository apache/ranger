package com.xasecure.view;
/*
 * Copyright (c) 2014 XASecure
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of
 * XASecure ("Confidential Information").  You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with XASecure
 */

/**
 * Audit Log for Policy Export
 * 
 */

import java.util.*;

import com.xasecure.common.*;
import com.xasecure.common.view.*;

import com.xasecure.common.*;
import com.xasecure.json.JsonDateSerializer;

import com.xasecure.view.*;

import javax.xml.bind.annotation.*;

import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

@JsonAutoDetect(getterVisibility=Visibility.NONE, setterVisibility=Visibility.NONE, fieldVisibility=Visibility.ANY)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL )
@JsonIgnoreProperties(ignoreUnknown=true)
@XmlRootElement
public class VXPolicyExportAudit extends VXDataObject implements java.io.Serializable {
	private static final long serialVersionUID = 1L;


	/**
	 * XA Agent IP Address
	 */
	protected String clientIP;
	/**
	 * XA Agent Id
	 */
	protected String agentId;
	/**
	 * Last update timestamp in request
	 */
	protected Long requestedEpoch;
	/**
	 * Date and time of the last policy update
	 */
	@JsonSerialize(using=JsonDateSerializer.class)
	protected Date lastUpdated;
	/**
	 * Id of the Asset
	 */
	protected String repositoryName;
	/**
	 * JSON of the policies exported
	 */
	protected String exportedJson;
	/**
	 * HTTP Response Code
	 */
	protected int httpRetCode;

	/**
	 * Default constructor. This will set all the attributes to default value.
	 */
	public VXPolicyExportAudit ( ) {
	}

	/**
	 * This method sets the value to the member attribute <b>clientIP</b>.
	 * You cannot set null to the attribute.
	 * @param clientIP Value to set member attribute <b>clientIP</b>
	 */
	public void setClientIP( String clientIP ) {
		this.clientIP = clientIP;
	}

	/**
	 * Returns the value for the member attribute <b>clientIP</b>
	 * @return String - value of member attribute <b>clientIP</b>.
	 */
	public String getClientIP( ) {
		return this.clientIP;
	}

	/**
	 * This method sets the value to the member attribute <b>agentId</b>.
	 * You cannot set null to the attribute.
	 * @param agentId Value to set member attribute <b>agentId</b>
	 */
	public void setAgentId( String agentId ) {
		this.agentId = agentId;
	}

	/**
	 * Returns the value for the member attribute <b>agentId</b>
	 * @return String - value of member attribute <b>agentId</b>.
	 */
	public String getAgentId( ) {
		return this.agentId;
	}

	/**
	 * This method sets the value to the member attribute <b>requestedEpoch</b>.
	 * You cannot set null to the attribute.
	 * @param requestedEpoch Value to set member attribute <b>requestedEpoch</b>
	 */
	public void setRequestedEpoch( Long requestedEpoch ) {
		this.requestedEpoch = requestedEpoch;
	}

	/**
	 * Returns the value for the member attribute <b>requestedEpoch</b>
	 * @return Long - value of member attribute <b>requestedEpoch</b>.
	 */
	public Long getRequestedEpoch( ) {
		return this.requestedEpoch;
	}

	/**
	 * This method sets the value to the member attribute <b>lastUpdated</b>.
	 * You cannot set null to the attribute.
	 * @param lastUpdated Value to set member attribute <b>lastUpdated</b>
	 */
	public void setLastUpdated( Date lastUpdated ) {
		this.lastUpdated = lastUpdated;
	}

	/**
	 * Returns the value for the member attribute <b>lastUpdated</b>
	 * @return Date - value of member attribute <b>lastUpdated</b>.
	 */
	public Date getLastUpdated( ) {
		return this.lastUpdated;
	}

	/**
	 * This method sets the value to the member attribute <b>repositoryName</b>.
	 * You cannot set null to the attribute.
	 * @param repositoryName Value to set member attribute <b>repositoryName</b>
	 */
	public void setRepositoryName( String repositoryName ) {
		this.repositoryName = repositoryName;
	}

	/**
	 * Returns the value for the member attribute <b>repositoryName</b>
	 * @return String - value of member attribute <b>repositoryName</b>.
	 */
	public String getRepositoryName( ) {
		return this.repositoryName;
	}

	/**
	 * This method sets the value to the member attribute <b>exportedJson</b>.
	 * You cannot set null to the attribute.
	 * @param exportedJson Value to set member attribute <b>exportedJson</b>
	 */
	public void setExportedJson( String exportedJson ) {
		this.exportedJson = exportedJson;
	}

	/**
	 * Returns the value for the member attribute <b>exportedJson</b>
	 * @return String - value of member attribute <b>exportedJson</b>.
	 */
	public String getExportedJson( ) {
		return this.exportedJson;
	}

	/**
	 * This method sets the value to the member attribute <b>httpRetCode</b>.
	 * You cannot set null to the attribute.
	 * @param httpRetCode Value to set member attribute <b>httpRetCode</b>
	 */
	public void setHttpRetCode( int httpRetCode ) {
		this.httpRetCode = httpRetCode;
	}

	/**
	 * Returns the value for the member attribute <b>httpRetCode</b>
	 * @return int - value of member attribute <b>httpRetCode</b>.
	 */
	public int getHttpRetCode( ) {
		return this.httpRetCode;
	}

	@Override
	public int getMyClassType( ) {
	    return AppConstants.CLASS_TYPE_XA_POLICY_EXPORT_AUDIT;
	}

	/**
	 * This return the bean content in string format
	 * @return formatedStr
	*/
	public String toString( ) {
		String str = "VXPolicyExportAudit={";
		str += super.toString();
		str += "clientIP={" + clientIP + "} ";
		str += "agentId={" + agentId + "} ";
		str += "requestedEpoch={" + requestedEpoch + "} ";
		str += "lastUpdated={" + lastUpdated + "} ";
		str += "repositoryName={" + repositoryName + "} ";
		str += "exportedJson={" + exportedJson + "} ";
		str += "httpRetCode={" + httpRetCode + "} ";
		str += "}";
		return str;
	}
}
