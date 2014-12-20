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

package org.apache.ranger.plugin.model;

import java.util.ArrayList;
import java.util.List;

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
public class RangerServiceDef extends RangerBaseModelObject implements java.io.Serializable {
	private static final long serialVersionUID = 1L;

	private String                         name             = null;
	private String                         implClass        = null;
	private String                         label            = null;
	private String                         description      = null;
	private String                         rbKeyLabel       = null;
	private String                         rbKeyDescription = null;
	private List<RangerServiceConfigDef>   configs          = null;
	private List<RangerResourceDef>        resources        = null;
	private List<RangerAccessTypeDef>      accessTypes      = null;
	private List<RangerPolicyConditionDef> policyConditions = null;
	private List<RangerEnumDef>            enums            = null;


	public RangerServiceDef() {
		this(null, null, null, null, null, null, null, null, null);
	}

	public RangerServiceDef(String name, String implClass, String label, String description, List<RangerServiceConfigDef> configs, List<RangerResourceDef> resources, List<RangerAccessTypeDef> accessTypes, List<RangerPolicyConditionDef> policyConditions, List<RangerEnumDef> enums) {
		super();

		setName(name);
		setImplClass(implClass);
		setLabel(label);
		setDescription(description);
		setConfigs(configs);
		setResources(resources);
		setAccessTypes(accessTypes);
		setPolicyConditions(policyConditions);
		setEnums(enums);
	}

	public void updateFrom(RangerServiceDef other) {
		setName(other.getName());
		setImplClass(other.getImplClass());
		setLabel(other.getLabel());
		setDescription(other.getDescription());
		setConfigs(other.getConfigs());
		setResources(other.getResources());
		setAccessTypes(other.getAccessTypes());
		setPolicyConditions(other.getPolicyConditions());
		setEnums(other.getEnums());
	}

	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param name the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @return the implClass
	 */
	public String getImplClass() {
		return implClass;
	}

	/**
	 * @param implClass the implClass to set
	 */
	public void setImplClass(String implClass) {
		this.implClass = implClass;
	}

	/**
	 * @return the label
	 */
	public String getLabel() {
		return label;
	}

	/**
	 * @param label the label to set
	 */
	public void setLabel(String label) {
		this.label = label;
	}

	/**
	 * @return the description
	 */
	public String getDescription() {
		return description;
	}

	/**
	 * @param description the description to set
	 */
	public void setDescription(String description) {
		this.description = description;
	}

	/**
	 * @return the rbKeyLabel
	 */
	public String getRbKeyLabel() {
		return rbKeyLabel;
	}

	/**
	 * @param rbKeyLabel the rbKeyLabel to set
	 */
	public void setRbKeyLabel(String rbKeyLabel) {
		this.rbKeyLabel = rbKeyLabel;
	}

	/**
	 * @return the rbKeyDescription
	 */
	public String getRbKeyDescription() {
		return rbKeyDescription;
	}

	/**
	 * @param rbKeyDescription the rbKeyDescription to set
	 */
	public void setRbKeyDescription(String rbKeyDescription) {
		this.rbKeyDescription = rbKeyDescription;
	}

	/**
	 * @return the configs
	 */
	public List<RangerServiceConfigDef> getConfigs() {
		return configs;
	}

	/**
	 * @param configs the configs to set
	 */
	public void setConfigs(List<RangerServiceConfigDef> configs) {
		this.configs = new ArrayList<RangerServiceConfigDef>();

		if(configs != null) {
			for(RangerServiceConfigDef config : configs) {
				this.configs.add(config);
			}
		}
	}

	/**
	 * @return the resources
	 */
	public List<RangerResourceDef> getResources() {
		return resources;
	}

	/**
	 * @param resources the resources to set
	 */
	public void setResources(List<RangerResourceDef> resources) {
		this.resources = new ArrayList<RangerResourceDef>();

		if(resources != null) {
			for(RangerResourceDef resource : resources) {
				this.resources.add(resource);
			}
		}
	}

	/**
	 * @return the accessTypes
	 */
	public List<RangerAccessTypeDef> getAccessTypes() {
		return accessTypes;
	}

	/**
	 * @param accessTypes the accessTypes to set
	 */
	public void setAccessTypes(List<RangerAccessTypeDef> accessTypes) {
		this.accessTypes = new ArrayList<RangerAccessTypeDef>();

		if(accessTypes != null) {
			for(RangerAccessTypeDef accessType : accessTypes) {
				this.accessTypes.add(accessType);
			}
		}
	}

	/**
	 * @return the policyConditions
	 */
	public List<RangerPolicyConditionDef> getPolicyConditions() {
		return policyConditions;
	}

	/**
	 * @param policyConditions the policyConditions to set
	 */
	public void setPolicyConditions(List<RangerPolicyConditionDef> policyConditions) {
		this.policyConditions = new ArrayList<RangerPolicyConditionDef>();

		if(policyConditions != null) {
			for(RangerPolicyConditionDef policyCondition : policyConditions) {
				this.policyConditions.add(policyCondition);
			}
		}
	}

	/**
	 * @return the enums
	 */
	public List<RangerEnumDef> getEnums() {
		return enums;
	}

	/**
	 * @param enums the enums to set
	 */
	public void setEnums(List<RangerEnumDef> enums) {
		this.enums = new ArrayList<RangerEnumDef>();

		if(enums != null) {
			for(RangerEnumDef enum1 : enums) {
				this.enums.add(enum1);
			}
		}
	}

	@Override
	public String toString( ) {
		StringBuilder sb = new StringBuilder();

		toString(sb);

		return sb.toString();
	}

	public StringBuilder toString(StringBuilder sb) {
		sb.append("RangerServiceDef={");

		super.toString(sb);

		sb.append("name={").append(name).append("} ");
		sb.append("implClass={").append(implClass).append("} ");
		sb.append("label={").append(label).append("} ");
		sb.append("description={").append(description).append("} ");
		sb.append("rbKeyLabel={").append(rbKeyLabel).append("} ");
		sb.append("rbKeyDescription={").append(rbKeyDescription).append("} ");

		sb.append("configs={");
		if(configs != null) {
			for(RangerServiceConfigDef config : configs) {
				if(config != null) {
					config.toString(sb);
				}
			}
		}
		sb.append("} ");

		sb.append("resources={");
		if(resources != null) {
			for(RangerResourceDef resource : resources) {
				if(resource != null) {
					resource.toString(sb);
				}
			}
		}
		sb.append("} ");

		sb.append("accessTypes={");
		if(accessTypes != null) {
			for(RangerAccessTypeDef accessType : accessTypes) {
				if(accessType != null) {
					accessType.toString(sb);
				}
			}
		}
		sb.append("} ");

		sb.append("policyConditions={");
		if(policyConditions != null) {
			for(RangerPolicyConditionDef policyCondition : policyConditions) {
				if(policyCondition != null) {
					policyCondition.toString(sb);
				}
			}
		}
		sb.append("} ");

		sb.append("enums={");
		if(enums != null) {
			for(RangerEnumDef e : enums) {
				if(e != null) {
					e.toString(sb);
				}
			}
		}
		sb.append("} ");

		sb.append("}");

		return sb;
	}


	public static class RangerEnumDef implements java.io.Serializable {
		private static final long serialVersionUID = 1L;

		private String                     name         = null;
		private List<RangerEnumElementDef> elements     = null;
		private Integer                    defaultIndex = null;


		public RangerEnumDef() {
			this(null, null, null);
		}

		public RangerEnumDef(String name, List<RangerEnumElementDef> elements, Integer defaultIndex) {
			setName(name);
			setElements(elements);
			setDefaultIndex(defaultIndex);
		}

		/**
		 * @return the name
		 */
		public String getName() {
			return name;
		}

		/**
		 * @param name the name to set
		 */
		public void setName(String name) {
			this.name = name;
		}

		/**
		 * @return the elements
		 */
		public List<RangerEnumElementDef> getElements() {
			return elements;
		}

		/**
		 * @param elements the elements to set
		 */
		public void setElements(List<RangerEnumElementDef> elements) {
			this.elements = new ArrayList<RangerEnumElementDef>();

			if(elements != null) {
				for(RangerEnumElementDef element : elements) {
					this.elements.add(element);
				}
			}
		}

		/**
		 * @return the defaultIndex
		 */
		public Integer getDefaultIndex() {
			return defaultIndex;
		}

		/**
		 * @param defaultIndex the defaultIndex to set
		 */
		public void setDefaultIndex(Integer defaultIndex) {
			this.defaultIndex = (defaultIndex != null && this.elements.size() > defaultIndex) ? defaultIndex : 0;
		}

		@Override
		public String toString( ) {
			StringBuilder sb = new StringBuilder();

			toString(sb);

			return sb.toString();
		}

		public StringBuilder toString(StringBuilder sb) {
			sb.append("RangerEnumDef={");
			sb.append("name={").append(name).append("} ");
			sb.append("elements={");
			if(elements != null) {
				for(RangerEnumElementDef element : elements) {
					if(element != null) {
						element.toString(sb);
					}
				}
			}
			sb.append("} ");
			sb.append("defaultIndex={").append(defaultIndex).append("} ");
			sb.append("}");

			return sb;
		}
	}


	public static class RangerEnumElementDef implements java.io.Serializable {
		private static final long serialVersionUID = 1L;
		
		private String name       = null;
		private String label      = null;
		private String rbKeyLabel = null;


		public RangerEnumElementDef() {
			this(null, null, null);
		}

		public RangerEnumElementDef(String name, String label, String rbKeyLabel) {
			setName(name);
			setLabel(label);
			setRbKeyLabel(rbKeyLabel);
		}

		/**
		 * @return the name
		 */
		public String getName() {
			return name;
		}

		/**
		 * @param name the name to set
		 */
		public void setName(String name) {
			this.name = name;
		}

		/**
		 * @return the label
		 */
		public String getLabel() {
			return label;
		}

		/**
		 * @param label the label to set
		 */
		public void setLabel(String label) {
			this.label = label;
		}

		/**
		 * @return the rbKeyLabel
		 */
		public String getRbKeyLabel() {
			return rbKeyLabel;
		}

		/**
		 * @param rbKeyLabel the rbKeyLabel to set
		 */
		public void setRbKeyLabel(String rbKeyLabel) {
			this.rbKeyLabel = rbKeyLabel;
		}

		@Override
		public String toString( ) {
			StringBuilder sb = new StringBuilder();

			toString(sb);

			return sb.toString();
		}

		public StringBuilder toString(StringBuilder sb) {
			sb.append("RangerEnumElementDef={");
			sb.append("name={").append(name).append("} ");
			sb.append("label={").append(label).append("} ");
			sb.append("rbKeyLabel={").append(rbKeyLabel).append("} ");
			sb.append("}");

			return sb;
		}
	}


	public static class RangerServiceConfigDef implements java.io.Serializable {
		private static final long serialVersionUID = 1L;

		private String  name             = null;
		private String  type             = null;
		private String  subType          = null;
		private Boolean mandatory        = null;
		private String  defaultValue     = null;
		private String  label            = null;
		private String  description      = null;
		private String  rbKeyLabel       = null;
		private String  rbKeyDescription = null;


		public RangerServiceConfigDef() {
			this(null, null, null, null, null, null, null, null, null);
		}

		public RangerServiceConfigDef(String name, String type, String subType, Boolean mandatory, String defaultValue, String label, String description, String rbKeyLabel, String rbKeyDescription) {
			setName(name);
			setType(type);
			setSubType(subType);
			setMandatory(mandatory);
			setDefaultValue(defaultValue);
			setLabel(label);
			setDescription(description);
			setRbKeyLabel(rbKeyLabel);
			setRbKeyDescription(rbKeyDescription);
		}

		/**
		 * @return the name
		 */
		public String getName() {
			return name;
		}

		/**
		 * @param name the name to set
		 */
		public void setName(String name) {
			this.name = name;
		}

		/**
		 * @return the type
		 */
		public String getType() {
			return type;
		}

		/**
		 * @param type the type to set
		 */
		public void setType(String type) {
			this.type = type;
		}

		/**
		 * @return the type
		 */
		public String getSubType() {
			return subType;
		}

		/**
		 * @param type the type to set
		 */
		public void setSubType(String subType) {
			this.subType = subType;
		}

		/**
		 * @return the mandatory
		 */
		public Boolean getMandatory() {
			return mandatory;
		}

		/**
		 * @param mandatory the mandatory to set
		 */
		public void setMandatory(Boolean mandatory) {
			this.mandatory = mandatory == null ? Boolean.FALSE : mandatory;
		}

		/**
		 * @return the defaultValue
		 */
		public String getDefaultValue() {
			return defaultValue;
		}

		/**
		 * @param defaultValue the defaultValue to set
		 */
		public void setDefaultValue(String defaultValue) {
			this.defaultValue = defaultValue;
		}

		/**
		 * @return the label
		 */
		public String getLabel() {
			return label;
		}

		/**
		 * @param label the label to set
		 */
		public void setLabel(String label) {
			this.label = label;
		}

		/**
		 * @return the description
		 */
		public String getDescription() {
			return description;
		}

		/**
		 * @param description the description to set
		 */
		public void setDescription(String description) {
			this.description = description;
		}

		/**
		 * @return the rbKeyLabel
		 */
		public String getRbKeyLabel() {
			return rbKeyLabel;
		}

		/**
		 * @param rbKeyLabel the rbKeyLabel to set
		 */
		public void setRbKeyLabel(String rbKeyLabel) {
			this.rbKeyLabel = rbKeyLabel;
		}

		/**
		 * @return the rbKeyDescription
		 */
		public String getRbKeyDescription() {
			return rbKeyDescription;
		}

		/**
		 * @param rbKeyDescription the rbKeyDescription to set
		 */
		public void setRbKeyDescription(String rbKeyDescription) {
			this.rbKeyDescription = rbKeyDescription;
		}

		@Override
		public String toString( ) {
			StringBuilder sb = new StringBuilder();

			toString(sb);

			return sb.toString();
		}

		public StringBuilder toString(StringBuilder sb) {
			sb.append("RangerServiceConfigDef={");
			sb.append("name={").append(name).append("} ");
			sb.append("type={").append(type).append("} ");
			sb.append("subType={").append(subType).append("} ");
			sb.append("mandatory={").append(mandatory).append("} ");
			sb.append("defaultValue={").append(defaultValue).append("} ");
			sb.append("label={").append(label).append("} ");
			sb.append("description={").append(description).append("} ");
			sb.append("rbKeyLabel={").append(rbKeyLabel).append("} ");
			sb.append("rbKeyDescription={").append(rbKeyDescription).append("} ");
			sb.append("}");

			return sb;
		}
	}


	public static class RangerResourceDef implements java.io.Serializable {
		private static final long serialVersionUID = 1L;

		private String  name               = null;
		private Integer level              = null;
		private String  parent             = null;
		private Boolean mandatory          = null;
		private Boolean lookupSupported    = null;
		private Boolean recursiveSupported = null;
		private Boolean excludesSupported  = null;
		private String  matcher            = null;
		private String  matcherOptions     = null;
		private String  label              = null;
		private String  description        = null;
		private String  rbKeyLabel         = null;
		private String  rbKeyDescription   = null;


		public RangerResourceDef() {
			this(null, null, null, null, null, null, null, null, null, null, null, null, null);
		}

		public RangerResourceDef(String name, Integer level, String parent, Boolean mandatory, Boolean lookupSupported, Boolean recursiveSupported, Boolean excludesSupported, String matcher, String matcherOptions, String label, String description, String rbKeyLabel, String rbKeyDescription) {
			setName(name);
			setLevel(level);
			setParent(parent);
			setMandatory(mandatory);
			setLookupSupported(lookupSupported);
			setRecursiveSupported(recursiveSupported);
			setExcludesSupported(excludesSupported);
			setMatcher(matcher);
			setMatcher(matcherOptions);
			setLabel(label);
			setDescription(description);
			setRbKeyLabel(rbKeyLabel);
			setRbKeyDescription(rbKeyDescription);
		}

		/**
		 * @return the name
		 */
		public String getName() {
			return name;
		}

		/**
		 * @param name the name to set
		 */
		public void setName(String name) {
			this.name = name;
		}

		/**
		 * @return the level
		 */
		public Integer getLevel() {
			return level;
		}

		/**
		 * @param level the level to set
		 */
		public void setLevel(Integer level) {
			this.level = level == null ? 1 : level;
		}

		/**
		 * @return the parent
		 */
		public String getParent() {
			return parent;
		}

		/**
		 * @param parent the parent to set
		 */
		public void setParent(String parent) {
			this.parent = parent;
		}

		/**
		 * @return the mandatory
		 */
		public Boolean getMandatory() {
			return mandatory;
		}

		/**
		 * @param mandatory the mandatory to set
		 */
		public void setMandatory(Boolean mandatory) {
			this.mandatory = mandatory == null ? Boolean.FALSE : mandatory;
		}

		/**
		 * @return the lookupSupported
		 */
		public Boolean getLookupSupported() {
			return lookupSupported;
		}

		/**
		 * @param lookupSupported the lookupSupported to set
		 */
		public void setLookupSupported(Boolean lookupSupported) {
			this.lookupSupported = lookupSupported == null ? Boolean.FALSE : lookupSupported;
		}

		/**
		 * @return the recursiveSupported
		 */
		public Boolean getRecursiveSupported() {
			return recursiveSupported;
		}

		/**
		 * @param recursiveSupported the recursiveSupported to set
		 */
		public void setRecursiveSupported(Boolean recursiveSupported) {
			this.recursiveSupported = recursiveSupported == null ? Boolean.FALSE : recursiveSupported;
		}

		/**
		 * @return the excludesSupported
		 */
		public Boolean getExcludesSupported() {
			return excludesSupported;
		}

		/**
		 * @param excludesSupported the excludesSupported to set
		 */
		public void setExcludesSupported(Boolean excludesSupported) {
			this.excludesSupported = excludesSupported == null ? Boolean.FALSE : excludesSupported;
		}

		/**
		 * @return the matcher
		 */
		public String getMatcher() {
			return matcher;
		}

		/**
		 * @param matcher the matcher to set
		 */
		public void setMatcher(String matcher) {
			this.matcher = matcher;
		}

		/**
		 * @return the matcher
		 */
		public String getMatcherOptions() {
			return matcherOptions;
		}

		/**
		 * @param matcher the matcher to set
		 */
		public void setMatcherOptions(String matcherOptions) {
			this.matcherOptions = matcherOptions;
		}

		/**
		 * @return the label
		 */
		public String getLabel() {
			return label;
		}

		/**
		 * @param label the label to set
		 */
		public void setLabel(String label) {
			this.label = label;
		}

		/**
		 * @return the description
		 */
		public String getDescription() {
			return description;
		}

		/**
		 * @param description the description to set
		 */
		public void setDescription(String description) {
			this.description = description;
		}

		/**
		 * @return the rbKeyLabel
		 */
		public String getRbKeyLabel() {
			return rbKeyLabel;
		}

		/**
		 * @param rbKeyLabel the rbKeyLabel to set
		 */
		public void setRbKeyLabel(String rbKeyLabel) {
			this.rbKeyLabel = rbKeyLabel;
		}

		/**
		 * @return the rbKeyDescription
		 */
		public String getRbKeyDescription() {
			return rbKeyDescription;
		}

		/**
		 * @param rbKeyDescription the rbKeyDescription to set
		 */
		public void setRbKeyDescription(String rbKeyDescription) {
			this.rbKeyDescription = rbKeyDescription;
		}

		@Override
		public String toString( ) {
			StringBuilder sb = new StringBuilder();

			toString(sb);

			return sb.toString();
		}

		public StringBuilder toString(StringBuilder sb) {
			sb.append("RangerResourceDef={");
			sb.append("name={").append(name).append("} ");
			sb.append("level={").append(level).append("} ");
			sb.append("parent={").append(parent).append("} ");
			sb.append("mandatory={").append(mandatory).append("} ");
			sb.append("lookupSupported={").append(lookupSupported).append("} ");
			sb.append("recursiveSupported={").append(recursiveSupported).append("} ");
			sb.append("excludesSupported={").append(excludesSupported).append("} ");
			sb.append("matcher={").append(matcher).append("} ");
			sb.append("matcherOptions={").append(matcherOptions).append("} ");
			sb.append("label={").append(label).append("} ");
			sb.append("description={").append(description).append("} ");
			sb.append("rbKeyLabel={").append(rbKeyLabel).append("} ");
			sb.append("rbKeyDescription={").append(rbKeyDescription).append("} ");
			sb.append("}");

			return sb;
		}
	}


	public static class RangerAccessTypeDef implements java.io.Serializable {
		private static final long serialVersionUID = 1L;

		private String name       = null;
		private String label      = null;
		private String rbKeyLabel = null;


		public RangerAccessTypeDef() {
			this(null, null, null);
		}

		public RangerAccessTypeDef(String name, String label, String rbKeyLabel) {
			setName(name);
			setLabel(label);
			setRbKeyLabel(rbKeyLabel);
		}

		/**
		 * @return the name
		 */
		public String getName() {
			return name;
		}

		/**
		 * @param name the name to set
		 */
		public void setName(String name) {
			this.name = name;
		}

		/**
		 * @return the label
		 */
		public String getLabel() {
			return label;
		}

		/**
		 * @param label the label to set
		 */
		public void setLabel(String label) {
			this.label = label;
		}

		/**
		 * @return the rbKeyLabel
		 */
		public String getRbKeyLabel() {
			return rbKeyLabel;
		}

		/**
		 * @param rbKeyLabel the rbKeyLabel to set
		 */
		public void setRbKeyLabel(String rbKeyLabel) {
			this.rbKeyLabel = rbKeyLabel;
		}

		@Override
		public String toString( ) {
			StringBuilder sb = new StringBuilder();

			toString(sb);

			return sb.toString();
		}

		public StringBuilder toString(StringBuilder sb) {
			sb.append("RangerAccessTypeDef={");
			sb.append("name={").append(name).append("} ");
			sb.append("label={").append(label).append("} ");
			sb.append("rbKeyLabel={").append(rbKeyLabel).append("} ");
			sb.append("}");

			return sb;
		}
	}


	public static class RangerPolicyConditionDef implements java.io.Serializable {
		private static final long serialVersionUID = 1L;

		private String name             = null;
		private String evalClass        = null;
		private String label            = null;
		private String description      = null;
		private String rbKeyLabel       = null;
		private String rbKeyDescription = null;


		public RangerPolicyConditionDef() {
			this(null, null, null, null, null, null);
		}

		public RangerPolicyConditionDef(String name, String evalClass) {
			this(name, evalClass, null, null, null, null);
		}

		public RangerPolicyConditionDef(String name, String evalClass, String label) {
			this(name, evalClass, label, null, null, null);
		}

		public RangerPolicyConditionDef(String name, String evalClass, String label, String description) {
			this(name, evalClass, label, description, null, null);
		}

		public RangerPolicyConditionDef(String name, String evalClass, String label, String description, String rbKeyLabel, String rbKeyDescription) {
			setName(name);
			setEvalClass(evalClass);
			setLabel(label);
			setDescription(description);
			setRbKeyLabel(rbKeyLabel);
			setRbKeyDescription(rbKeyDescription);
		}

		/**
		 * @return the name
		 */
		public String getName() {
			return name;
		}

		/**
		 * @param name the name to set
		 */
		public void setName(String name) {
			this.name = name;
		}

		/**
		 * @return the evalClass
		 */
		public String getEvalClass() {
			return evalClass;
		}

		/**
		 * @param evalClass the evalClass to set
		 */
		public void setEvalClass(String evalClass) {
			this.evalClass = evalClass;
		}

		/**
		 * @return the label
		 */
		public String getLabel() {
			return label;
		}

		/**
		 * @param label the label to set
		 */
		public void setLabel(String label) {
			this.label = label;
		}

		/**
		 * @return the description
		 */
		public String getDescription() {
			return description;
		}

		/**
		 * @param description the description to set
		 */
		public void setDescription(String description) {
			this.description = description;
		}

		/**
		 * @return the rbKeyLabel
		 */
		public String getRbKeyLabel() {
			return rbKeyLabel;
		}

		/**
		 * @param rbKeyLabel the rbKeyLabel to set
		 */
		public void setRbKeyLabel(String rbKeyLabel) {
			this.rbKeyLabel = rbKeyLabel;
		}

		/**
		 * @return the rbKeyDescription
		 */
		public String getRbKeyDescription() {
			return rbKeyDescription;
		}

		/**
		 * @param rbKeyDescription the rbKeyDescription to set
		 */
		public void setRbKeyDescription(String rbKeyDescription) {
			this.rbKeyDescription = rbKeyDescription;
		}

		@Override
		public String toString( ) {
			StringBuilder sb = new StringBuilder();

			toString(sb);

			return sb.toString();
		}

		public StringBuilder toString(StringBuilder sb) {
			sb.append("RangerPolicyConditionDef={");
			sb.append("name={").append(name).append("} ");
			sb.append("evalClass={").append(evalClass).append("} ");
			sb.append("label={").append(label).append("} ");
			sb.append("description={").append(description).append("} ");
			sb.append("rbKeyLabel={").append(rbKeyLabel).append("} ");
			sb.append("rbKeyDescription={").append(rbKeyDescription).append("} ");
			sb.append("}");

			return sb;
		}
	}
}
