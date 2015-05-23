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

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.MediaType;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.collections.Predicate;
import org.apache.commons.collections.PredicateUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.ProviderUtils;
import org.apache.log4j.Logger;
import org.apache.ranger.common.ContextUtil;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.RangerConfigUtil;
import org.apache.ranger.common.SortField;
import org.apache.ranger.common.StringUtil;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.util.KeySearchFilter;
import org.apache.ranger.view.VXKmsKey;
import org.apache.ranger.view.VXKmsKeyList;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;

@Component
public class KmsKeyMgr {

	static final Logger logger = Logger.getLogger(KmsKeyMgr.class);
	
	private static final String KMS_KEY_LIST_URI  		= "v1/keys/names?user.name=${userName}";			//GET
	private static final String KMS_ADD_KEY_URI  		= "v1/keys?user.name=${userName}";					//POST
	private static final String KMS_ROLL_KEY_URI 		= "v1/key/${alias}?user.name=${userName}";			//POST
	private static final String KMS_DELETE_KEY_URI 		= "v1/key/${alias}?user.name=${userName}";			//DELETE
	private static final String KMS_KEY_METADATA_URI 	= "v1/key/${alias}/_metadata?user.name=${userName}";  //GET
	private static final String KMS_URL_CONFIG 			= "provider"; 
	private static Map<String, String> providerList = new HashMap<String, String>(); 
	private static int nextProvider = 0;
	
	@Autowired
	ServiceDBStore svcStore;	
	
	@Autowired
	RESTErrorUtil restErrorUtil;
	
	@Autowired
	RangerConfigUtil configUtil;
	
	@SuppressWarnings("unchecked")
	public VXKmsKeyList searchKeys(String repoName){
		String providers[] = null;
		try {
			providers = getKMSURL(repoName);
		} catch (Exception e) {
			logger.error("getKey(" + repoName + ") failed", e);
		}
		List<VXKmsKey> vXKeys = new ArrayList<VXKmsKey>();
		VXKmsKeyList vxKmsKeyList = new VXKmsKeyList();
		List<String> keys = null;
		String connProvider = null;
		for (int i = 0; i < providers.length; i++) {
			Client c = getClient();
			String currentUserLoginId = ContextUtil.getCurrentUserLoginId();
			String keyLists = KMS_KEY_LIST_URI.replaceAll(
					Pattern.quote("${userName}"), currentUserLoginId);
			connProvider = providers[i];
			String uri = providers[i]
					+ (providers[i].endsWith("/") ? keyLists : ("/" + keyLists));

			WebResource r = c.resource(uri);
			try {
				String response = r.accept(MediaType.APPLICATION_JSON_TYPE)
						.get(String.class);
				Gson gson = new GsonBuilder().create();
				logger.debug(" Search Key RESPONSE: [" + response + "]");

				keys = gson.fromJson(response, List.class);
				break;
			} catch (Exception e) {
				if (e instanceof UniformInterfaceException || i == providers.length - 1)
					throw e;								
				else
					continue;
			}
		}
		if (keys != null && keys.size() > 0) {
			for (String name : keys) {
				VXKmsKey key = getKeyFromUri(connProvider, name);
				vXKeys.add(key);
			}
			vxKmsKeyList.setResultSize(vXKeys.size());
			vxKmsKeyList.setTotalCount(vXKeys.size());
			vxKmsKeyList.setStartIndex(0);
			vxKmsKeyList.setPageSize(vXKeys.size());
		}
		vxKmsKeyList.setVXKeys(vXKeys);
		return vxKmsKeyList;
	}

	public VXKmsKey rolloverKey(String provider, VXKmsKey vXKey){
		String providers[] = null;
		try {
			providers = getKMSURL(provider);
		} catch (Exception e) {
			logger.error("rolloverKey(" + provider + ", " + vXKey.getName()
					+ ") failed", e);
		}
		VXKmsKey ret = null;
		for (int i = 0; i < providers.length; i++) {
			Client c = getClient();
			String rollRest = KMS_ROLL_KEY_URI.replaceAll(
					Pattern.quote("${alias}"), vXKey.getName());
			String currentUserLoginId = ContextUtil.getCurrentUserLoginId();
			rollRest = rollRest.replaceAll(Pattern.quote("${userName}"),
					currentUserLoginId);
			String uri = providers[i]
					+ (providers[i].endsWith("/") ? rollRest : ("/" + rollRest));
			WebResource r = c.resource(uri);
			Gson gson = new GsonBuilder().create();
			String jsonString = gson.toJson(vXKey);
			try {
				String response = r.accept(MediaType.APPLICATION_JSON_TYPE)
						.type(MediaType.APPLICATION_JSON_TYPE)
						.post(String.class, jsonString);
				logger.debug("Roll RESPONSE: [" + response + "]");
				ret = gson.fromJson(response, VXKmsKey.class);
				break;
			} catch (Exception e) {
				if (e instanceof UniformInterfaceException || i == providers.length - 1)
					throw e;								
				else
					continue;
			}
		}
		return ret;
	}

	public void deleteKey(String provider, String name){
		String providers[] = null;
		try {
			providers = getKMSURL(provider);
		} catch (Exception e) {
			logger.error("deleteKey(" + provider + ", " + name + ") failed", e);
		}
		for (int i = 0; i < providers.length; i++) {
			Client c = getClient();
			String deleteRest = KMS_DELETE_KEY_URI.replaceAll(
					Pattern.quote("${alias}"), name);
			String currentUserLoginId = ContextUtil.getCurrentUserLoginId();
			deleteRest = deleteRest.replaceAll(Pattern.quote("${userName}"),
					currentUserLoginId);
			String uri = providers[i]
					+ (providers[i].endsWith("/") ? deleteRest
							: ("/" + deleteRest));
			WebResource r = c.resource(uri);
			try {
				String response = r.delete(String.class) ;
				logger.debug("delete RESPONSE: [" + response + "]") ;	
				break;
			} catch (Exception e) {
				if (e instanceof UniformInterfaceException || i == providers.length - 1)
					throw e;								
				else
					continue;
			}
		}			
	}

	public VXKmsKey createKey(String provider, VXKmsKey vXKey){
		String providers[] = null;
		try {
			providers = getKMSURL(provider);
		} catch (Exception e) {
			logger.error("createKey(" + provider + ", " + vXKey.getName()
					+ ") failed", e);
		}
		VXKmsKey ret = null;
		for (int i = 0; i < providers.length; i++) {
			Client c = getClient();
			String currentUserLoginId = ContextUtil.getCurrentUserLoginId();
			String createRest = KMS_ADD_KEY_URI.replaceAll(
					Pattern.quote("${userName}"), currentUserLoginId);
			String uri = providers[i]
					+ (providers[i].endsWith("/") ? createRest
							: ("/" + createRest));
			WebResource r = c.resource(uri);
			Gson gson = new GsonBuilder().create();
			String jsonString = gson.toJson(vXKey);
			try {
				String response = r.accept(MediaType.APPLICATION_JSON_TYPE)
						.type(MediaType.APPLICATION_JSON_TYPE)
						.post(String.class, jsonString);
				logger.debug("Create RESPONSE: [" + response + "]");
				ret = gson.fromJson(response, VXKmsKey.class);
				return ret;
			} catch (Exception e) {
				if (e instanceof UniformInterfaceException || i == providers.length - 1)
					throw e;								
				else
					continue;
			}
		}
		return ret;	
	}
	
	public VXKmsKey getKey(String provider, String name){
		String providers[] = null;
		try {
			providers = getKMSURL(provider);
		} catch (Exception e) {
			logger.error("getKey(" + provider + ", " + name + ") failed", e);
		}
		for (int i = 0; i < providers.length; i++) {
			Client c = getClient();
			String keyRest = KMS_KEY_METADATA_URI.replaceAll(
					Pattern.quote("${alias}"), name);
			String currentUserLoginId = ContextUtil.getCurrentUserLoginId();
			keyRest = keyRest.replaceAll(Pattern.quote("${userName}"),
					currentUserLoginId);
			String uri = providers[i]
					+ (providers[i].endsWith("/") ? keyRest : ("/" + keyRest));
			WebResource r = c.resource(uri);
			try {
				String response = r.accept(MediaType.APPLICATION_JSON_TYPE)
						.get(String.class);
				Gson gson = new GsonBuilder().create();
				logger.debug("RESPONSE: [" + response + "]");
				VXKmsKey key = gson.fromJson(response, VXKmsKey.class);
				return key;
			} catch (Exception e) {
				if (e instanceof UniformInterfaceException || i == providers.length - 1)
					throw e;								
				else
					continue;
			}
		}
		return null;
	}
	
	public VXKmsKey getKeyFromUri(String provider, String name) {
		Client c = getClient();
		String keyRest = KMS_KEY_METADATA_URI.replaceAll(
				Pattern.quote("${alias}"), name);
		String currentUserLoginId = ContextUtil.getCurrentUserLoginId();
		keyRest = keyRest.replaceAll(Pattern.quote("${userName}"),
				currentUserLoginId);
		String uri = provider + (provider.endsWith("/") ? keyRest : ("/" + keyRest));
		WebResource r = c.resource(uri);
		String response = r.accept(MediaType.APPLICATION_JSON_TYPE).get(String.class);
		Gson gson = new GsonBuilder().create();
		logger.debug("RESPONSE: [" + response + "]");
		VXKmsKey key = gson.fromJson(response, VXKmsKey.class);
		return key;			
	}
	
	private String[] getKMSURL(String name) throws Exception{
		String providers[] = null;
		RangerService rangerService = null;
		try {
			rangerService = svcStore.getServiceByName(name);
			String kmsUrl = rangerService.getConfigs().get(KMS_URL_CONFIG);
			String dbKmsUrl = kmsUrl;
			if(providerList.containsKey(kmsUrl)){
				kmsUrl = providerList.get(kmsUrl);				
			}else{
				providerList.put(kmsUrl, kmsUrl);
			}
			providers = createProvider(dbKmsUrl,kmsUrl);
		} catch (Exception excp) {
			logger.error("getServiceByName(" + name + ") failed", excp);
			throw new Exception("getServiceByName(" + name + ") failed", excp);
		}

		if (rangerService == null || providers == null) {
			throw new Exception("Provider " + name + " not found");
		}
		return providers;
	}
	
	private String[] createProvider(String dbKmsUrl, String uri) throws IOException,URISyntaxException {		
		URI providerUri = new URI(uri);
		URL origUrl = new URL(extractKMSPath(providerUri).toString());
		String authority = origUrl.getAuthority();
		// 	check for ';' which delimits the backup hosts
		if (Strings.isNullOrEmpty(authority)) {
			throw new IOException("No valid authority in kms uri [" + origUrl+ "]");
		}
		// 	Check if port is present in authority
		// 	In the current scheme, all hosts have to run on the same port
		int port = -1;
		String hostsPart = authority;
		if (authority.contains(":")) {
			String[] t = authority.split(":");
			try {
				port = Integer.parseInt(t[1]);
			} catch (Exception e) {
				throw new IOException("Could not parse port in kms uri ["
				+ origUrl + "]");
			}
			hostsPart = t[0];
		}
		return createProvider(dbKmsUrl, providerUri, origUrl, port, hostsPart);
	}

	private static Path extractKMSPath(URI uri) throws MalformedURLException,IOException {
		return ProviderUtils.unnestUri(uri);
	}

	private String[] createProvider(String dbkmsUrl, URI providerUri, URL origUrl, int port,
			String hostsPart) throws IOException {
		String[] hosts = hostsPart.split(";");
		String[] providers = new String[hosts.length];
		if (hosts.length == 1) {
			providers[0] = origUrl.toString();
		} else {
			String providerNext=providerUri.getScheme()+"://"+origUrl.getProtocol()+"@";
			for(int i=nextProvider; i<hosts.length; i++){
				providerNext = providerNext+hosts[i];
				if(i!=(hosts.length-1)){
					providerNext = providerNext+";";
				}
			}
			for(int i=0; i<nextProvider; i++){
				providerNext = providerNext+";"+hosts[i];
			}
			if(nextProvider != hosts.length-1){
				nextProvider = nextProvider+1;
			}else{
				nextProvider = 0;
			}
			providerNext = providerNext +":"+port+origUrl.getPath();
			providerList.put(dbkmsUrl, providerNext);
			for (int i = 0; i < hosts.length; i++) {
				try {
					String url = origUrl.getProtocol()+"://"+hosts[i]+":"+port+origUrl.getPath();
					providers[i] = new URI(url).toString();
				} catch (URISyntaxException e) {
					throw new IOException("Could not Prase KMS URL..", e);
				}
			}
		}
		return providers;
	}

	private synchronized Client getClient() {
		Client ret = null; 
		ClientConfig cc = new DefaultClientConfig();
		cc.getProperties().put(ClientConfig.PROPERTY_FOLLOW_REDIRECTS, true);
		ret = Client.create(cc);	
		return ret ;
	}	
	
	public VXKmsKeyList getFilteredKeyList(HttpServletRequest request, VXKmsKeyList vXKmsKeyList){
		List<SortField> sortFields = new ArrayList<SortField>();
		sortFields.add(new SortField(KeySearchFilter.KEY_NAME, KeySearchFilter.KEY_NAME));
		
		KeySearchFilter filter = getKeySearchFilter(request, sortFields);
		
		Predicate pred = getPredicate(filter);
		
		if(pred != null) {
			CollectionUtils.filter(vXKmsKeyList.getVXKeys(), pred);
		}
		return vXKmsKeyList;
	}
	
	private Predicate getPredicate(KeySearchFilter filter) {
		if(filter == null || filter.isEmpty()) {
			return null;
		}

		List<Predicate> predicates = new ArrayList<Predicate>();

		addPredicateForKeyName(filter.getParam(KeySearchFilter.KEY_NAME), predicates);
		
		Predicate ret = CollectionUtils.isEmpty(predicates) ? null : PredicateUtils.allPredicate(predicates);

		return ret;
	}
	
	private Predicate addPredicateForKeyName(final String name, List<Predicate> predicates) {
			if(StringUtils.isEmpty(name)) {
				return null;
			}

			Predicate ret = new Predicate() {
				@Override
				public boolean evaluate(Object object) {
					if(object == null) {
						return false;
					}

					boolean ret = false;

					if(object instanceof VXKmsKey) {
						VXKmsKey vXKmsKey = (VXKmsKey)object;
						if(StringUtils.isEmpty(vXKmsKey.getName())) {
							ret = true;
						}else{
							ret = vXKmsKey.getName().contains(name);
						}
					} else {
						ret = true;
					}

					return ret;
				}
			};

			if(predicates != null) {
				predicates.add(ret);
			}
				
			return ret;
	}
		
	private KeySearchFilter getKeySearchFilter(HttpServletRequest request, List<SortField> sortFields) {
		if (request == null) {
			return null;
		}
		KeySearchFilter ret = new KeySearchFilter();

		if (MapUtils.isEmpty(request.getParameterMap())) {
			ret.setParams(new HashMap<String, String>());
		}

		ret.setParam(KeySearchFilter.KEY_NAME, request.getParameter(KeySearchFilter.KEY_NAME));
		extractCommonCriteriasForFilter(request, ret, sortFields);
		return ret;
	}
	
	private KeySearchFilter extractCommonCriteriasForFilter(HttpServletRequest request, KeySearchFilter ret, List<SortField> sortFields) {
		int startIndex = restErrorUtil.parseInt(request.getParameter(KeySearchFilter.START_INDEX), 0,
				"Invalid value for parameter startIndex", MessageEnums.INVALID_INPUT_DATA, null,
				KeySearchFilter.START_INDEX);
		ret.setStartIndex(startIndex);

		int pageSize = restErrorUtil.parseInt(request.getParameter(KeySearchFilter.PAGE_SIZE),
				configUtil.getDefaultMaxRows(), "Invalid value for parameter pageSize",
				MessageEnums.INVALID_INPUT_DATA, null, KeySearchFilter.PAGE_SIZE);
		ret.setMaxRows(pageSize);

		ret.setGetCount(restErrorUtil.parseBoolean(request.getParameter("getCount"), true));
		String sortBy = restErrorUtil.validateString(request.getParameter(KeySearchFilter.SORT_BY),
				StringUtil.VALIDATION_ALPHA, "Invalid value for parameter sortBy", MessageEnums.INVALID_INPUT_DATA,
				null, KeySearchFilter.SORT_BY);

		boolean sortSet = false;
		if (!StringUtils.isEmpty(sortBy)) {
			for (SortField sortField : sortFields) {
				if (sortField.getParamName().equalsIgnoreCase(sortBy)) {
					ret.setSortBy(sortField.getParamName());
					String sortType = restErrorUtil.validateString(request.getParameter("sortType"),
							StringUtil.VALIDATION_ALPHA, "Invalid value for parameter sortType",
							MessageEnums.INVALID_INPUT_DATA, null, "sortType");
					ret.setSortType(sortType);
					sortSet = true;
					break;
				}
			}
		}

		if (!sortSet && !StringUtils.isEmpty(sortBy)) {
			logger.info("Invalid or unsupported sortBy field passed. sortBy=" + sortBy, new Throwable());
		}
		
		if(ret.getParams() == null) {
			ret.setParams(new HashMap<String, String>());
		}
		return ret;
	}
}
