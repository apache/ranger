/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ranger.view;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility;
import org.codehaus.jackson.map.annotate.JsonSerialize;

@JsonAutoDetect(getterVisibility=Visibility.NONE, setterVisibility=Visibility.NONE, fieldVisibility=Visibility.ANY)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL )
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class VXMetricAuditDetailsCount implements java.io.Serializable {
	private static final long serialVersionUID = 1L;
	
	protected Long solrIndexCount;
	protected VXMetricServiceCount accessEventsCount;
	protected VXMetricServiceCount denialEventsCount;
	
	/**
	 * Default constructor. This will set all the attributes to default value.
	 */
	public VXMetricAuditDetailsCount() {
	}
	
	/**
	 * @return the solrIndexCount
	 */
	public Long getSolrIndexCount() {
		return solrIndexCount;
	}

	/**
	 * @param solrIndexCount the solrIndexCount to set
	 */
	public void setSolrIndexCount(Long solrIndexCount) {
		this.solrIndexCount = solrIndexCount;
	}

	/**
	 * @return the accessEventsCount
	 */
	public VXMetricServiceCount getAccessEventsCount() {
		return accessEventsCount;
	}

	/**
	 * @param accessEventsCount the accessEventsCount to set
	 */
	public void setAccessEventsCount(VXMetricServiceCount accessEventsCount) {
		this.accessEventsCount = accessEventsCount;
	}

	/**
	 * @return the denialEventsCount
	 */
	public VXMetricServiceCount getDenialEventsCount() {
		return denialEventsCount;
	}

	/**
	 * @param denialEventsCount the denialEventsCount to set
	 */
	public void setDenialEventsCount(VXMetricServiceCount denialEventsCount) {
		this.denialEventsCount = denialEventsCount;
	}

	@Override
	public String toString() {
		String str = "VXMetricAuditDetailsCount=[";
		str += "solrIndexCount={" + solrIndexCount + "},";
		str += "accessEventsCount={" + accessEventsCount.toString() + "}, ";
		str += "denialEventsCount={" + denialEventsCount.toString() + "} ";
		str += "]";
		return str;
	}
}
