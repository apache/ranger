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
package org.apache.ranger.patch.cliutil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.ranger.biz.AssetMgr;
import org.apache.ranger.biz.RangerBizUtil;
import org.apache.ranger.biz.ServiceDBStore;
import org.apache.ranger.biz.XUserMgr;
import org.apache.ranger.common.AppConstants;
import org.apache.ranger.common.RangerConstants;
import org.apache.ranger.common.SearchCriteria;
import org.apache.ranger.patch.BaseLoader;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerContextEnricherDef;
import org.apache.ranger.plugin.store.PList;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.util.CLIUtil;
import org.apache.ranger.view.VXAccessAuditList;
import org.apache.ranger.view.VXGroupList;
import org.apache.ranger.view.VXMetricContextEnricher;
import org.apache.ranger.view.VXMetricAuditDetailsCount;
import org.apache.ranger.view.VXMetricServiceCount;
import org.apache.ranger.view.VXMetricPolicyCount;
import org.apache.ranger.view.VXMetricUserGroupCount;
import org.apache.ranger.view.VXUserList;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

@Component
public class MetricUtil extends BaseLoader  {
	
	private static Logger logger = Logger.getLogger(MetricUtil.class);
	
	public static String metricType;
		
	@Autowired
	XUserMgr xUserMgr;
	
	@Autowired
	AssetMgr assetMgr;
	
	@Autowired
	ServiceDBStore svcStore;
	
	@Autowired
	RangerBizUtil xaBizUtil;
	
	public static void main(String[] args) {
		logger.getRootLogger().setLevel(Level.OFF);
		logger.info("MetricUtil : main()");
		try {
			MetricUtil loader = (MetricUtil) CLIUtil.getBean(MetricUtil.class);
			loader.init();
			if(args.length != 2){
				System.out.println("type: Incorrect Arguments usage : -type policies | audits | usergroup | services | database | contextenrichers | denyconditions");
			}else {
				if((!args[0].equalsIgnoreCase("-type")) && (!args[1].equalsIgnoreCase("policies") || !args[1].equalsIgnoreCase("audits") || !args[1].equalsIgnoreCase("usergroup") || !args[1].equalsIgnoreCase("services") || !args[1].equalsIgnoreCase("database") || !args[1].equalsIgnoreCase("contextenrichers") || !args[1].equalsIgnoreCase("denyconditions"))){
					System.out.println("type: Incorrect Arguments usage : -type policies | audits | usergroup | services | database | contextenrichers | denyconditions");	
				}else{
					metricType = args[1];
					if(logger.isDebugEnabled()){
						logger.debug("Metric Type : " + metricType);
					}
				}
			}
			while (loader.isMoreToProcess()) {
				loader.load();
			}
			logger.info("Load complete. Exiting!!!");
			System.exit(0);
		}
		catch (Exception e) {
			logger.error("Error loading", e);
			System.exit(1);
		}
	}
	
	@Override
	public void init() throws Exception {
		logger.info("==> MetricUtil.init()");
	}
	
	@Override
	public void execLoad() {
		logger.info("==> MetricUtil.execLoad()");
		metricCalculation(metricType);
		logger.info("<== MetricUtil.execLoad()");
	}

	@Override
	public void printStats() {		
	}
	
	private Object metricCalculation(String caseValue) {
		logger.info("Metric Type : " + caseValue);		
		try {
			SearchCriteria searchCriteria = new SearchCriteria();
			searchCriteria.setStartIndex(0);
			searchCriteria.setMaxRows(100);
			searchCriteria.setGetCount(true);
			searchCriteria.setSortType("asc");			
		
			switch (caseValue.toLowerCase()) {
				case "usergroup":
					try {
						VXGroupList VXGroupList = xUserMgr.searchXGroups(searchCriteria);

						long groupCount = VXGroupList.getTotalCount();

						ArrayList<String> userRoleList = new ArrayList<String>();
						userRoleList.add(RangerConstants.ROLE_SYS_ADMIN);
						userRoleList.add(RangerConstants.ROLE_KEY_ADMIN);
						userRoleList.add(RangerConstants.ROLE_USER);
						searchCriteria.addParam("userRoleList", userRoleList);
						VXUserList VXUserList = xUserMgr.searchXUsers(searchCriteria);
						long userCount = VXUserList.getTotalCount();

						VXMetricUserGroupCount metricUserGroupCount = new VXMetricUserGroupCount();
						metricUserGroupCount.setUserCount(userCount);
						metricUserGroupCount.setGroupCount(groupCount);
						Gson gson = new GsonBuilder().create();
						final String jsonUserGroupCount = gson.toJson(metricUserGroupCount);
						System.out.println(jsonUserGroupCount);
					} catch (Exception e) {
						logger.error("Error for calculating Metric for usergroup : "+ e.getMessage());
					}
					break;								
				case "audits":
					try{
						VXMetricAuditDetailsCount auditObj = new VXMetricAuditDetailsCount();
						VXMetricServiceCount deniedCountObj =  getAuditsCount(0);
						auditObj.setDenialEventsCount(deniedCountObj);
						VXMetricServiceCount allowedCountObj = getAuditsCount(1);
						auditObj.setAccessEventsCount(allowedCountObj);
						long totalAuditsCount = deniedCountObj.getTotalCount() + allowedCountObj.getTotalCount();				
						auditObj.setSolrIndexCount(totalAuditsCount);
						Gson gson = new GsonBuilder().create();
						final String jsonAudit = gson.toJson(auditObj);
						System.out.println(jsonAudit);
					}catch (Exception e) {
						logger.error("Error for calculating Metric for audits : "+e.getMessage());
					}
					break;	
				case "services" : 	
					try{
						SearchFilter serviceFilter = new SearchFilter();
						serviceFilter.setMaxRows(200);
						serviceFilter.setStartIndex(0);
						serviceFilter.setGetCount(true);
						serviceFilter.setSortBy("serviceId");
						serviceFilter.setSortType("asc");
						VXMetricServiceCount vXMetricServiceCount = new VXMetricServiceCount();
						PList<RangerService> paginatedSvcs = svcStore.getPaginatedServices(serviceFilter);
						long totalServiceCount = paginatedSvcs.getTotalCount();				
						List<RangerService> rangerServiceList = paginatedSvcs.getList();
						Map<String,Long> services = new HashMap<String,Long>();						
						for (Object rangerService:rangerServiceList) {
							RangerService RangerServiceObj = (RangerService) rangerService;
							String serviceName = RangerServiceObj.getType();
							if(!(services.containsKey(serviceName)))
							{
								serviceFilter.setParam("serviceType",serviceName);
								PList<RangerService> paginatedSvcscount = svcStore.getPaginatedServices(serviceFilter);
								services.put(serviceName,paginatedSvcscount.getTotalCount());
							}
						}
						vXMetricServiceCount.setServiceCountList(services);
						vXMetricServiceCount.setTotalCount(totalServiceCount);
						Gson gson = new GsonBuilder().create();
						final String jsonServices = gson.toJson(vXMetricServiceCount);
						System.out.println(jsonServices);
					}catch (Exception e) {
						logger.error("Error for calculating Metric for services : "+e.getMessage());
					}
					break;					
				case "policies" :   
					try{
						SearchFilter policyFilter = new SearchFilter();
						policyFilter.setMaxRows(200);
						policyFilter.setStartIndex(0);
						policyFilter.setGetCount(true);
						policyFilter.setSortBy("serviceId");
						policyFilter.setSortType("asc");
						VXMetricPolicyCount vXMetricPolicyCount = new VXMetricPolicyCount();
						PList<RangerPolicy> paginatedSvcsList = svcStore.getPaginatedPolicies(policyFilter);
						vXMetricPolicyCount.setTotalCount(paginatedSvcsList.getTotalCount());
						Map<String,VXMetricServiceCount> servicesWithPolicy = new HashMap<String,VXMetricServiceCount>();
						for(int k = 2;k >= 0;k--)
						{
							String serviceType = String.valueOf(k);
							VXMetricServiceCount vXMetricServiceCount = getVXMetricServiceCount(serviceType); 							
							if(k == 2)
								servicesWithPolicy.put("row_filtering_policies", vXMetricServiceCount);
							else if(k == 1)
								servicesWithPolicy.put("masking_policies", vXMetricServiceCount);
							else if(k == 0)
								servicesWithPolicy.put("resource_policy", vXMetricServiceCount);							
						}
						boolean tagFlag = false;
						if(tagFlag == false)
						{
							policyFilter.setParam("serviceType","tag");
							PList<RangerPolicy> policiestype = svcStore.getPaginatedPolicies(policyFilter);
							Map<String,Long> tagMap= new HashMap<String,Long>();
							long tagCount = policiestype.getTotalCount();
							tagMap.put("tag",tagCount);
							VXMetricServiceCount vXMetricServiceCount = new VXMetricServiceCount();
							vXMetricServiceCount.setServiceCountList(tagMap);
							vXMetricServiceCount.setTotalCount(tagCount);
							servicesWithPolicy.put("tag_based_policies",vXMetricServiceCount);
							tagFlag = true;
						}
						vXMetricPolicyCount.setvXMetricServiceCount(servicesWithPolicy);
						Gson gson = new GsonBuilder().create();
						final String jsonPolicies = gson.toJson(vXMetricPolicyCount);
						System.out.println(jsonPolicies);
					}catch (Exception e) {
						logger.error("Error for calculating Metric for policies : "+e.getMessage());
					}				
					break;
				case "database" :
					try{						
						int dbFlavor = RangerBizUtil.getDBFlavor();						
						String dbFlavourType = "Unknow ";
						if(dbFlavor == AppConstants.DB_FLAVOR_MYSQL){
							dbFlavourType = "MYSQL ";
						}else if(dbFlavor == AppConstants.DB_FLAVOR_ORACLE){
							dbFlavourType = "ORACLE ";
						}else if(dbFlavor == AppConstants.DB_FLAVOR_POSTGRES){
							dbFlavourType = "POSTGRES ";
						}else if(dbFlavor == AppConstants.DB_FLAVOR_SQLANYWHERE){
							dbFlavourType = "SQLANYWHERE ";
						}else if(dbFlavor == AppConstants.DB_FLAVOR_SQLSERVER){
							dbFlavourType = "SQLSERVER ";
						}
						String dbDetail = dbFlavourType + xaBizUtil.getDBVersion();
						Gson gson = new GsonBuilder().create();
						final String jsonDBDetail = gson.toJson(dbDetail);
						System.out.println(jsonDBDetail);						
					}catch (Exception e) {
						logger.error("Error for calculating Metric for database : "+e.getMessage());
					}
					break;
				case "contextenrichers":
					try
					{
						SearchFilter filter = new SearchFilter();
					    filter.setStartIndex(0);
					    VXMetricContextEnricher serviceWithContextEnrichers= new VXMetricContextEnricher();
					    PList<RangerServiceDef> paginatedSvcDefs = svcStore.getPaginatedServiceDefs(filter);
					    List<RangerServiceDef> repoTypeList = paginatedSvcDefs.getList();
					    if(repoTypeList != null){
					    	for (RangerServiceDef repoType:repoTypeList) 
					    	{
					    		RangerServiceDef rangerServiceDefObj = (RangerServiceDef)repoType;
					    		String name = rangerServiceDefObj.getName();
					    		List<RangerContextEnricherDef> contextEnrichers = rangerServiceDefObj.getContextEnrichers();							
					    		if(contextEnrichers != null && contextEnrichers.size() > 0){
					    			serviceWithContextEnrichers.setServiceName(name);
					    			serviceWithContextEnrichers.setTotalCount(contextEnrichers.size());								
					    		}
					    	}
					    }
					    Gson gson = new GsonBuilder().create();
						final String jsonContextEnrichers = gson.toJson(serviceWithContextEnrichers);
						System.out.println(jsonContextEnrichers);
					}
					catch (Exception e) {
						logger.error("Error for calculating Metric for contextenrichers : "+e.getMessage());
					}
					break;
				case "denyconditions":
	                try {
	                    SearchFilter policyFilter1 = new SearchFilter();
	                    policyFilter1.setMaxRows(200);
	                    policyFilter1.setStartIndex(0);
	                    policyFilter1.setGetCount(true);
	                    policyFilter1.setSortBy("serviceId");
	                    policyFilter1.setSortType("asc");
	                    
	                    int denyCount = 0;
	                    Map<String, Integer> denyconditionsonMap = new HashMap<String, Integer>();
	                    PList<RangerServiceDef> paginatedSvcDefs = svcStore.getPaginatedServiceDefs(policyFilter1);
	                    if(paginatedSvcDefs != null){
	                    	List<RangerServiceDef> rangerServiceDef = paginatedSvcDefs.getList();
	                    	if(rangerServiceDef != null && rangerServiceDef.size() > 0)
		                    {
		                        for(int i=0; i<rangerServiceDef.size(); i++)
		                        {
		                        	if(rangerServiceDef.get(i) != null){
		                        		String serviceDef = rangerServiceDef.get(i).getName();
		                        		if (!StringUtils.isEmpty(serviceDef)){
		                        			policyFilter1.setParam("serviceType", serviceDef);
		                        			PList<RangerPolicy> policiesList = svcStore.getPaginatedPolicies(policyFilter1);
		                        			if(policiesList != null && policiesList.getListSize() > 0){
		                        				int policyListCount = policiesList.getListSize();
		                        				if (policyListCount > 0 && policiesList.getList() != null) {
		                        					List<RangerPolicy> policies = policiesList.getList();
		                        					for(int j=0; j<policies.size(); j++){
		                        						if(policies.get(j) != null){
		                        							List<RangerPolicyItem> policyItem = policies.get(j).getDenyPolicyItems();
		                        							if(policyItem != null && policyItem.size() > 0){
		                        								if(denyconditionsonMap.get(serviceDef) != null){
		                        									denyCount = denyconditionsonMap.get(serviceDef) + denyCount + policyItem.size();
		                        								}else{
		                        									denyCount = denyCount + policyItem.size();
		                        								}
		                        							}
		                        							List<RangerPolicyItem> policyItemExclude = policies.get(j).getDenyExceptions();
		                        							if(policyItemExclude != null && policyItemExclude.size() > 0){
		                        								if(denyconditionsonMap.get(serviceDef) != null){
		                        									denyCount = denyconditionsonMap.get(serviceDef) + denyCount + policyItemExclude.size();
		                        								}else{
		                        									denyCount = denyCount + policyItemExclude.size();
		                        								}
		                        							}
		                        						}
		                        					}
		                        				}
		            	                    }
		                        			policyFilter1.removeParam("serviceType");
		                        		}		        	                        
		                        		denyconditionsonMap.put(serviceDef, denyCount);
		                        		denyCount = 0;
		                        	}
		                        }
		                    }	
	                    }
	                    Gson gson = new GsonBuilder().create();
	                    String jsonContextDenyCondtionOn = gson.toJson(denyconditionsonMap);
	                    System.out.println(jsonContextDenyCondtionOn);
	                } catch (Exception e) {
	                    logger.error("Error for calculating Metric for denyconditions : "+ e.getMessage());
	                }
	                break;
				default:
					System.out.println("type: Incorrect Arguments usage : -type policies | audits | usergroup | services | database | contextenrichers | denyconditions");
					logger.info("Please enter the valid arguments for Metric Calculation");
					break;
			}
		} catch(Exception e) {
			logger.error("Error for calculating Metric : "+e.getMessage());
		}		
		return null;		
	}
	
	private  VXMetricServiceCount getVXMetricServiceCount(String serviceType) throws Exception 
	{
		SearchFilter policyFilter1 = new SearchFilter();
		policyFilter1.setMaxRows(200);
		policyFilter1.setStartIndex(0);
		policyFilter1.setGetCount(true);
		policyFilter1.setSortBy("serviceId");
		policyFilter1.setSortType("asc");
		policyFilter1.setParam("policyType",serviceType);
		PList<RangerPolicy> policies = svcStore.getPaginatedPolicies(policyFilter1);
		PList<RangerService> paginatedSvcsSevice = svcStore.getPaginatedServices(policyFilter1);		
		
		List<RangerService> rangerServiceList = paginatedSvcsSevice.getList();
		
		Map<String,Long> servicesforPolicyType = new HashMap<String,Long>();
		long tagCount = 0;
		for (Object rangerService : rangerServiceList) {
			RangerService rangerServiceObj = (RangerService) rangerService;
			String serviceName = rangerServiceObj.getType();
			if(!(servicesforPolicyType.containsKey(serviceName)))
			{
				policyFilter1.setParam("serviceType",serviceName);
				PList<RangerPolicy> policiestype = svcStore.getPaginatedPolicies(policyFilter1);
				long count = policiestype.getTotalCount();
				if(count != 0)
				{
					if(!serviceName.equalsIgnoreCase("tag")){
						servicesforPolicyType.put(serviceName,count);
					}
					else{
						tagCount=count;
					}
				}
			}
		}
		VXMetricServiceCount vXMetricServiceCount = new VXMetricServiceCount();
		vXMetricServiceCount.setServiceCountList(servicesforPolicyType);		
		long totalCountOfPolicyType = policies.getTotalCount()-tagCount;		
		vXMetricServiceCount.setTotalCount(totalCountOfPolicyType);
		return vXMetricServiceCount;
	}
	
	private VXMetricServiceCount getAuditsCount(int accessResult) throws Exception {
	
		long totalCountOfAudits = 0;
		SearchFilter filter = new SearchFilter();
		filter.setStartIndex(0);
		
		Map<String,Long> servicesRepoType = new HashMap<String,Long>();
		VXMetricServiceCount vXMetricServiceCount = new VXMetricServiceCount();
		PList<RangerServiceDef> paginatedSvcDefs = svcStore.getPaginatedServiceDefs(filter);
		Iterable<RangerServiceDef> repoTypeGet = paginatedSvcDefs.getList();
		for (Object repo:repoTypeGet) {
			RangerServiceDef rangerServiceDefObj = (RangerServiceDef)repo;
			long id = rangerServiceDefObj.getId();
			String serviceRepoName = rangerServiceDefObj.getName();
			SearchCriteria searchCriteriaWithType = new SearchCriteria();
			searchCriteriaWithType.getParamList().put("repoType",id);
			searchCriteriaWithType.getParamList().put("accessResult", accessResult);
			VXAccessAuditList vXAccessAuditListwithType = assetMgr.getAccessLogs(searchCriteriaWithType);
			long toltalCountOfRepo = vXAccessAuditListwithType.getTotalCount();
			if(toltalCountOfRepo != 0)
			{
				servicesRepoType.put(serviceRepoName, toltalCountOfRepo);
				totalCountOfAudits += toltalCountOfRepo;
			}
		}	
		vXMetricServiceCount.setServiceCountList(servicesRepoType);
		vXMetricServiceCount.setTotalCount(totalCountOfAudits);
		return vXMetricServiceCount;
	}
}
