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

package org.apache.ranger.tagsync.sink.tagadmin;

import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.SecureClientLogin;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.admin.client.datatype.RESTResponse;
import org.apache.ranger.tagsync.model.TagSink;
import org.apache.ranger.plugin.util.RangerRESTClient;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.plugin.util.ServiceTags;
import org.apache.ranger.tagsync.process.TagSyncConfig;
import javax.security.auth.Subject;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TagAdminRESTSink implements TagSink, Runnable {
	private static final Log LOG = LogFactory.getLog(TagAdminRESTSink.class);

	private static final String REST_PREFIX = "/service";
	private static final String MODULE_PREFIX = "/tags";

	private static final String REST_MIME_TYPE_JSON = "application/json";

	private static final String REST_URL_IMPORT_SERVICETAGS_RESOURCE = REST_PREFIX + MODULE_PREFIX + "/importservicetags/";

	private static final String AUTH_TYPE_KERBEROS = "kerberos";

	private long rangerAdminConnectionCheckInterval;

	private RangerRESTClient tagRESTClient = null;

	private BlockingQueue<UploadWorkItem> uploadWorkItems;
	
	private String authenticationType;	
	private String principal;
	private String keytab;
	private String nameRules;

	private Thread myThread = null;

	@Override
	public boolean initialize(Properties properties) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> TagAdminRESTSink.initialize()");
		}

		boolean ret = false;

		String restUrl       = TagSyncConfig.getTagAdminRESTUrl(properties);
		String sslConfigFile = TagSyncConfig.getTagAdminRESTSslConfigFile(properties);
		String userName = TagSyncConfig.getTagAdminUserName(properties);
		String password = TagSyncConfig.getTagAdminPassword(properties);
		rangerAdminConnectionCheckInterval = TagSyncConfig.getTagAdminConnectionCheckInterval(properties);
		authenticationType = TagSyncConfig.getAuthenticationType(properties);
		nameRules = TagSyncConfig.getNameRules(properties);
		principal = TagSyncConfig.getKerberosPrincipal(properties);
		keytab = TagSyncConfig.getKerberosKeytab(properties);

		if (LOG.isDebugEnabled()) {
			LOG.debug("restUrl=" + restUrl);
			LOG.debug("sslConfigFile=" + sslConfigFile);
			LOG.debug("userName=" + userName);
			LOG.debug("rangerAdminConnectionCheckInterval" + rangerAdminConnectionCheckInterval);
		}

		if (StringUtils.isNotBlank(restUrl)) {
			tagRESTClient = new RangerRESTClient(restUrl, sslConfigFile);
			if(isKerberosEnabled()) {
				Subject subject = null;
				try {
					subject = SecureClientLogin.loginUserFromKeytab(principal, keytab, nameRules);
				} catch(IOException exception) {
					LOG.error("Could not get Subject from principal:[" + principal + "], keytab:[" + keytab + "], nameRules:[" + nameRules + "]", exception);
				}
				if (subject != null) {
					try {
						UserGroupInformation.loginUserFromSubject(subject);
						ret = true;
					} catch (IOException exception) {
						LOG.error("Failed to get UGI from Subject:[" + subject + "]");
					}
				}
			} else {
				tagRESTClient.setBasicAuthInfo(userName, password);
				ret = true;
			}
		} else {
			LOG.error("No value specified for property 'ranger.tagsync.tagadmin.rest.url'!");
		}

		if (ret) {
			uploadWorkItems = new LinkedBlockingQueue<UploadWorkItem>();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== TagAdminRESTSink.initialize(), result=" + ret);
		}

		return ret;
	}

	@Override
	public ServiceTags upload(ServiceTags toUpload) throws Exception {

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> upload() ");
		}

		UploadWorkItem uploadWorkItem = new UploadWorkItem(toUpload);

		uploadWorkItems.put(uploadWorkItem);

		// Wait until message is successfully delivered
		ServiceTags ret = uploadWorkItem.waitForUpload();

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== upload()");
		}

		return ret;
	}

	private boolean isKerberosEnabled() {
		return !StringUtils.isEmpty(authenticationType) && authenticationType.trim().equalsIgnoreCase(AUTH_TYPE_KERBEROS) && SecureClientLogin.isKerberosCredentialExists(principal, keytab);
	}

	private ServiceTags doUpload(ServiceTags serviceTags) throws Exception {
			if(isKerberosEnabled()) {
				try{
					UserGroupInformation userGroupInformation = UserGroupInformation.getLoginUser();
					if (userGroupInformation != null) {
						try {
							userGroupInformation.checkTGTAndReloginFromKeytab();
						} catch (IOException ioe) {
							LOG.error("Error renewing TGT and relogin", ioe);
							userGroupInformation = null;
						}
					}
					if (userGroupInformation != null) {
						if (LOG.isDebugEnabled()) {
							LOG.debug("Using Principal = " + principal + ", keytab = " + keytab);
						}
						final ServiceTags serviceTag = serviceTags;
						ServiceTags ret = userGroupInformation.doAs(new PrivilegedAction<ServiceTags>() {
							@Override
							public ServiceTags run() {
								try {
									return uploadServiceTags(serviceTag);
								} catch (Exception e) {
									LOG.error("Upload of service-tags failed with message ", e);
								}
								return null;
							}
						});
						return ret;
					} else {
						LOG.error("Failed to get UserGroupInformation.getLoginUser()");
						return null; // This will cause retries !!!
					}
				}catch(Exception e){
					LOG.error("Upload of service-tags failed with message ", e);
				}
				return null;
			}else{
				return uploadServiceTags(serviceTags);
			}
	}
	
	private ServiceTags uploadServiceTags(ServiceTags serviceTags) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> doUpload()");
		}
		WebResource webResource = createWebResource(REST_URL_IMPORT_SERVICETAGS_RESOURCE);

		ClientResponse response    = webResource.accept(REST_MIME_TYPE_JSON).type(REST_MIME_TYPE_JSON).put(ClientResponse.class, tagRESTClient.toJson(serviceTags));

		if(response == null || response.getStatus() != HttpServletResponse.SC_NO_CONTENT) {

			RESTResponse resp = RESTResponse.fromClientResponse(response);

			LOG.error("Upload of service-tags failed with message " + resp.getMessage());

			if (response == null || resp.getHttpStatusCode() != HttpServletResponse.SC_BAD_REQUEST) {
				// NOT an application error
				throw new Exception("Upload of service-tags failed with response: " + response);
			}

		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== doUpload()");
		}

		return serviceTags;
	}

	private WebResource createWebResource(String url) {
		return createWebResource(url, null);
	}

	private WebResource createWebResource(String url, SearchFilter filter) {
		WebResource ret = tagRESTClient.getResource(url);

		if(filter != null && !MapUtils.isEmpty(filter.getParams())) {
			for(Map.Entry<String, String> e : filter.getParams().entrySet()) {
				String name  = e.getKey();
				String value = e.getValue();

				ret.queryParam(name, value);
			}
		}

		return ret;
	}

	@Override
	public boolean start() {

		myThread = new Thread(this);
		myThread.setDaemon(true);
		myThread.start();

		return true;
	}

	@Override
	public void stop() {
		if (myThread != null && myThread.isAlive()) {
			myThread.interrupt();
		}
	}

	@Override
	public void run() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagAdminRESTSink.run()");
		}

		while (true) {
			UploadWorkItem uploadWorkItem;

			try {
				uploadWorkItem = uploadWorkItems.take();

				ServiceTags toUpload = uploadWorkItem.getServiceTags();

				boolean doRetry;

				do {
					doRetry = false;

					try {
						ServiceTags uploaded = doUpload(toUpload);
						if (uploaded == null) { // Treat this as if an Exception is thrown by doUpload
							doRetry = true;
							Thread.sleep(rangerAdminConnectionCheckInterval);
						} else {
							// ServiceTags uploaded successfully
							uploadWorkItem.uploadCompleted(uploaded);
						}
					} catch (InterruptedException interrupted) {
						LOG.error("Caught exception..: ", interrupted);
						return;
					} catch (Exception exception) {
						doRetry = true;
						Thread.sleep(rangerAdminConnectionCheckInterval);
					}
				} while (doRetry);

			}
			catch (InterruptedException exception) {
				LOG.error("Interrupted..: ", exception);
				return;
			}
		}

	}

	static class UploadWorkItem {
		private ServiceTags serviceTags;
		private BlockingQueue<ServiceTags> uploadedServiceTags;

		ServiceTags getServiceTags() {
			return serviceTags;
		}

		ServiceTags waitForUpload() throws InterruptedException {
			return uploadedServiceTags.take();
		}

		void uploadCompleted(ServiceTags uploaded) throws InterruptedException {
			// ServiceTags uploaded successfully
			uploadedServiceTags.put(uploaded);
		}

		UploadWorkItem(ServiceTags serviceTags) {
			setServiceTags(serviceTags);
			uploadedServiceTags = new ArrayBlockingQueue<ServiceTags>(1);
		}

		void setServiceTags(ServiceTags serviceTags) {
			this.serviceTags = serviceTags;
		}

	}

}
