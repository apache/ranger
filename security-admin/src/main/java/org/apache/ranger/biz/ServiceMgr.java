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

package org.apache.ranger.biz;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.client.HadoopException;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.service.RangerBaseService;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.plugin.store.ServiceStore;
import org.apache.ranger.service.RangerServiceService;
import org.apache.ranger.view.VXMessage;
import org.apache.ranger.view.VXResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;


@Component
public class ServiceMgr {

	private static final Log LOG = LogFactory.getLog(ServiceMgr.class);
	
	@Autowired
	RangerServiceService rangerSvcService;
	
	@Autowired
	ServiceDBStore svcDBStore;
	
	public List<String> lookupResource(String serviceName, ResourceLookupContext context, ServiceStore svcStore) throws Exception {
		List<String> 	  ret = null;
		
		RangerService service = svcDBStore.getServiceByName(serviceName);
		
		Map<String, String> newConfigs = rangerSvcService.getConfigsWithDecryptedPassword(service);
		service.setConfigs(newConfigs);
		
		RangerBaseService svc = getRangerServiceByService(service, svcStore);

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceMgr.lookupResource for Service: (" + svc + "Context: " + context + ")");
		}

		if(svc != null) {
			ClassLoader clsLoader = Thread.currentThread().getContextClassLoader();

			try {
				Thread.currentThread().setContextClassLoader(svc.getClass().getClassLoader());

				ret = svc.lookupResource(context);
			} catch (Exception e) {
				LOG.error("==> ServiceMgr.lookupResource Error:" + e);
				throw e;
			} finally {
				Thread.currentThread().setContextClassLoader(clsLoader);
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceMgr.lookupResource for Response: (" + ret + ")");
		}

		return ret;
	}
	
	public VXResponse validateConfig(RangerService service, ServiceStore svcStore) throws Exception {
		VXResponse        ret = new VXResponse();
		
		Map<String, String> newConfigs = rangerSvcService.getConfigsWithDecryptedPassword(service);
		service.setConfigs(newConfigs);
		
		RangerBaseService svc = getRangerServiceByService(service, svcStore);

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceMgr.validateConfig for Service: (" + svc + ")");
		}

		if(svc != null) {
			ClassLoader clsLoader = Thread.currentThread().getContextClassLoader();

			try {
				Thread.currentThread().setContextClassLoader(svc.getClass().getClassLoader());

				HashMap<String, Object> responseData = svc.validateConfig();

				ret = generateResponseForTestConn(responseData, "");
			} catch (Exception e) {
				String msg = "Unable to connect repository with given config for " + svc.getServiceName();
						
				HashMap<String, Object> respData = new HashMap<String, Object>();
				if (e instanceof HadoopException) {
					respData = ((HadoopException) e).responseData;
				}
				ret = generateResponseForTestConn(respData, msg);
				LOG.error("==> ServiceMgr.validateConfig Error:" + e);
			} finally {
				Thread.currentThread().setContextClassLoader(clsLoader);
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceMgr.validateConfig for Response: (" + ret + ")");
		}

		return ret;
	}

	public RangerBaseService getRangerServiceByName(String serviceName, ServiceStore svcStore) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceMgr.getRangerServiceByName(" + serviceName + ")");
		}

		RangerBaseService ret     = null;
		RangerService     service = svcStore == null ? null : svcStore.getServiceByName(serviceName);

		if(service != null) {
			ret = getRangerServiceByService(service, svcStore);
		} else {
			LOG.warn("ServiceMgr.getRangerServiceByName(" + serviceName + "): could not find the service");
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceMgr.getRangerServiceByName(" + serviceName + "): " + ret);
		}

		return ret;
	}

	public RangerBaseService getRangerServiceByService(RangerService service, ServiceStore svcStore) throws Exception{
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceMgr.getRangerServiceByService(" + service + ")");
		}

		RangerBaseService ret         = null;
		String	          serviceType = service == null ? null : service.getType();

		if(! StringUtils.isEmpty(serviceType)) {
			RangerServiceDef serviceDef = svcStore == null ? null : svcStore.getServiceDefByName(serviceType);

			if(serviceDef != null) {
				Class<RangerBaseService> cls = getClassForServiceType(serviceDef);

				if(cls != null) {
					ret = cls.newInstance();

					ret.init(serviceDef, service);	
				} else {
					LOG.warn("ServiceMgr.getRangerServiceByService(" + service + "): could not find service class '" + serviceDef.getImplClass() + "'");
				}
			} else {
				LOG.warn("ServiceMgr.getRangerServiceByService(" + service + "): could not find the service-type '" + serviceType + "'");
			}
		} else {
			LOG.warn("ServiceMgr.getRangerServiceByService(" + service + "): could not find the service-type");
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceMgr.getRangerServiceByService(" + service + "): " + ret);
		}		

		return ret;
	}

	private static Map<String, Class<RangerBaseService>> serviceTypeClassMap = new HashMap<String, Class<RangerBaseService>>();

	@SuppressWarnings("unchecked")
	private Class<RangerBaseService> getClassForServiceType(RangerServiceDef serviceDef) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceMgr.getClassForServiceType(" + serviceDef + ")");
		}

		Class<RangerBaseService> ret = null;

		if(serviceDef != null) {
			String serviceType = serviceDef.getName();

			ret = serviceTypeClassMap.get(serviceType);

			if(ret == null) {
				synchronized(serviceTypeClassMap) {
					ret = serviceTypeClassMap.get(serviceType);

					if(ret == null) {
						String clsName = serviceDef.getImplClass();

						if(LOG.isDebugEnabled()) {
							LOG.debug("ServiceMgr.getClassForServiceType(" + serviceType + "): service-class " + clsName + " not found in cache");
						}

						URL[]          pluginFiles = getPluginFilesForServiceType(serviceType);
						URLClassLoader clsLoader   = new URLClassLoader(pluginFiles, Thread.currentThread().getContextClassLoader());

						try {
							Class<?> cls = Class.forName(clsName, true, clsLoader);

							ret = (Class<RangerBaseService>)cls;

							serviceTypeClassMap.put(serviceType, ret);
						} catch (Exception excp) {
							LOG.warn("ServiceMgr.getClassForServiceType(" + serviceType + "): failed to find service-class '" + clsName + "'. Resource lookup will not be available", excp);
							//Let's propagate the error
							throw new Exception(serviceType + " failed to find service class " + clsName + ". Resource lookup will not be available. Please make sure plugin jar is in the correct place.");
						}
					} else {
						if(LOG.isDebugEnabled()) {
							LOG.debug("ServiceMgr.getClassForServiceType(" + serviceType + "): service-class " + ret.getCanonicalName() + " found in cache");
						}
					}
				}
			} else {
				if(LOG.isDebugEnabled()) {
					LOG.debug("ServiceMgr.getClassForServiceType(" + serviceType + "): service-class " + ret.getCanonicalName() + " found in cache");
				}
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceMgr.getClassForServiceType(" + serviceDef + "): " + ret);
		}

		return ret;
	}

	private URL[] getPluginFilesForServiceType(String serviceType) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceMgr.getPluginFilesForServiceType(" + serviceType + ")");
		}

		List<URL> ret = new ArrayList<URL>();

		getFilesInDirectory("ranger-plugins/" + serviceType, ret);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceMgr.getPluginFilesForServiceType(" + serviceType + "): " + ret.size() + " files");
		}

		return ret.toArray(new URL[] { });
	}

	private void getFilesInDirectory(String dirPath, List<URL> files) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceMgr.getFilesInDirectory(" + dirPath + ")");
		}

		URL pluginJarPath = getClass().getClassLoader().getResource(dirPath);

		if(pluginJarPath != null && pluginJarPath.getProtocol().equals("file")) {
			try {
				File[] dirFiles = new File(pluginJarPath.toURI()).listFiles();

				if(dirFiles != null) {
					for(File dirFile : dirFiles) {
						try {
							URL jarPath = dirFile.toURI().toURL();

							LOG.warn("getFilesInDirectory('" + dirPath + "'): adding " + dirFile.getAbsolutePath());
	
							files.add(jarPath);
						} catch(Exception excp) {
							LOG.warn("getFilesInDirectory('" + dirPath + "'): failed to get URI for file " + dirFile.getAbsolutePath(), excp);
						}
					}
				}
			} catch(Exception excp) {
				LOG.warn("getFilesInDirectory('" + dirPath + "'): error", excp);
			}
		} else {
				LOG.warn("getFilesInDirectory('" + dirPath + "'): could not find directory in CLASSPATH");
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceMgr.getFilesInDirectory(" + dirPath + ")");
		}
	}

	private VXResponse generateResponseForTestConn(
			HashMap<String, Object> responseData, String msg) {
		VXResponse vXResponse = new VXResponse();

		Long objId = null;
		boolean connectivityStatus = false;
		int statusCode = VXResponse.STATUS_ERROR;
		String message = msg;
		String description = msg;
		String fieldName = null;

		if (responseData != null) {
			if (responseData.get("objectId") != null) {
				objId = Long.parseLong(responseData.get("objectId").toString());
			}
			if (responseData.get("connectivityStatus") != null) {
				connectivityStatus = Boolean.parseBoolean(responseData.get("connectivityStatus").toString());
			}
			if (connectivityStatus) {
				statusCode = VXResponse.STATUS_SUCCESS;
			}
			if (responseData.get("message") != null) {
				message = responseData.get("message").toString();
			}
			if (responseData.get("description") != null) {
				description = responseData.get("description").toString();
			}
			if (responseData.get("fieldName") != null) {
				fieldName = responseData.get("fieldName").toString();
			}
		}

		VXMessage vXMsg = new VXMessage();
		List<VXMessage> vXMsgList = new ArrayList<VXMessage>();
		vXMsg.setFieldName(fieldName);
		vXMsg.setMessage(message);
		vXMsg.setObjectId(objId);
		vXMsgList.add(vXMsg);

		vXResponse.setMessageList(vXMsgList);
		vXResponse.setMsgDesc(description);
		vXResponse.setStatusCode(statusCode);
		return vXResponse;
	}
}

