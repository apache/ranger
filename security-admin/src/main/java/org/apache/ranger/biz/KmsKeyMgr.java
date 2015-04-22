package org.apache.ranger.biz;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.MediaType;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.collections.Predicate;
import org.apache.commons.collections.PredicateUtils;
import org.apache.commons.lang.StringUtils;
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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
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
	
	@Autowired
	ServiceDBStore svcStore;	
	
	@Autowired
	RESTErrorUtil restErrorUtil;
	
	@Autowired
	RangerConfigUtil configUtil;
	
	public VXKmsKeyList searchKeys(String repoName){
		String provider = null;
		try {
			provider = getKMSURL(repoName);
		} catch (Exception e) {
			logger.error("getKey(" + repoName + ") failed", e);
		}
		Client c = getClient() ;
		String currentUserLoginId = ContextUtil.getCurrentUserLoginId();
		String keyLists = KMS_KEY_LIST_URI.replaceAll(Pattern.quote("${userName}"), currentUserLoginId);
		String uri = provider + (provider.endsWith("/") ? keyLists : ("/" + keyLists));		
		VXKmsKeyList vxKmsKeyList = new VXKmsKeyList();
		WebResource r = c.resource(uri) ;
		String response = r.accept(MediaType.APPLICATION_JSON_TYPE).get(String.class);
		Gson gson = new GsonBuilder().create() ;
		logger.debug(" Search Key RESPONSE: [" + response + "]") ;
		List<VXKmsKey> vXKeys = new ArrayList<VXKmsKey>();	    
		@SuppressWarnings("unchecked")
		List<String> keys = gson.fromJson(response, List.class) ;
		if(keys != null && keys.size() > 0){
			for(String name : keys){
				VXKmsKey key = getKey(repoName, name);
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
		try {
			provider = getKMSURL(provider);
		} catch (Exception e) {
			logger.error("rolloverKey(" + provider + ", "+ vXKey.getName() +") failed", e);
		}
		VXKmsKey ret = null ;
		Client c = getClient() ;
		String rollRest = KMS_ROLL_KEY_URI.replaceAll(Pattern.quote("${alias}"), vXKey.getName());
		String currentUserLoginId = ContextUtil.getCurrentUserLoginId();
		rollRest = rollRest.replaceAll(Pattern.quote("${userName}"), currentUserLoginId);
		String uri = provider + (provider.endsWith("/") ? rollRest : ("/" + rollRest));
		WebResource r = c.resource(uri) ;
		Gson gson = new GsonBuilder().create() ;
		String jsonString = gson.toJson(vXKey) ;
		String response = r.accept(MediaType.APPLICATION_JSON_TYPE).type(MediaType.APPLICATION_JSON_TYPE).post(String.class, jsonString) ;
		logger.debug("Roll RESPONSE: [" + response + "]") ;
		ret = gson.fromJson(response, VXKmsKey.class) ;
		return ret ;
	}

	public void deleteKey(String provider, String name){
		try {
			provider = getKMSURL(provider);
		} catch (Exception e) {
			logger.error("deleteKey(" + provider + ", "+ name +") failed", e);
		}
		Client c = getClient() ;
		String deleteRest = KMS_DELETE_KEY_URI.replaceAll(Pattern.quote("${alias}"), name);
		String currentUserLoginId = ContextUtil.getCurrentUserLoginId();
		deleteRest = deleteRest.replaceAll(Pattern.quote("${userName}"), currentUserLoginId);
		String uri = provider + (provider.endsWith("/") ? deleteRest : ("/" + deleteRest));
		WebResource r = c.resource(uri) ;
		ClientResponse response = r.delete(ClientResponse.class) ;
		logger.debug("delete RESPONSE: [" + response.toString() + "]") ;			
		if (response.getStatus() == 200) {
			logger.debug("Alias "+name+" deleted successfully");
		}		
	}

	public VXKmsKey createKey(String provider, VXKmsKey vXKey){
		try {
			provider = getKMSURL(provider);
		} catch (Exception e) {
			logger.error("createKey(" + provider + ", "+ vXKey.getName() +") failed", e);
		}
		VXKmsKey ret = null ;
		Client c = getClient() ;
		String currentUserLoginId = ContextUtil.getCurrentUserLoginId();
		String createRest = KMS_ADD_KEY_URI.replaceAll(Pattern.quote("${userName}"), currentUserLoginId);
		String uri = provider + (provider.endsWith("/") ? createRest : ("/" + createRest));
		WebResource r = c.resource(uri) ;
		Gson gson = new GsonBuilder().create() ;
		String jsonString = gson.toJson(vXKey) ;
        String response = r.accept(MediaType.APPLICATION_JSON_TYPE).type(MediaType.APPLICATION_JSON_TYPE).post(String.class, jsonString) ;
        logger.debug("Create RESPONSE: [" + response + "]") ;
        ret = gson.fromJson(response, VXKmsKey.class) ;
	    return ret ;		
	}
	
	public VXKmsKey getKey(String provider, String name){
		try {
			provider = getKMSURL(provider);
		} catch (Exception e) {
			logger.error("getKey(" + provider + ", "+ name +") failed", e);
		}
		Client c = getClient() ;
		String keyRest = KMS_KEY_METADATA_URI.replaceAll(Pattern.quote("${alias}"), name);
		String currentUserLoginId = ContextUtil.getCurrentUserLoginId();
		keyRest = keyRest.replaceAll(Pattern.quote("${userName}"), currentUserLoginId);
		String uri = provider + (provider.endsWith("/") ? keyRest : ("/" + keyRest));
		WebResource r = c.resource(uri) ;
		String response = r.accept(MediaType.APPLICATION_JSON_TYPE).get(String.class);
		Gson gson = new GsonBuilder().create() ;
		logger.debug("RESPONSE: [" + response + "]") ;
		VXKmsKey key = gson.fromJson(response, VXKmsKey.class) ;
		return key;
	}
	
	private String getKMSURL(String name) throws Exception{
		String provider = null;
		RangerService rangerService = null;
		try {
			rangerService = svcStore.getServiceByName(name);
			provider = rangerService.getConfigs().get(KMS_URL_CONFIG);
			provider = provider.replaceAll("kms://","");
			provider = provider.replaceAll("http@","http://");
		} catch(Exception excp) {
			logger.error("getServiceByName(" + name + ") failed", excp);
			throw new Exception("getServiceByName(" + name + ") failed", excp);
		}

		if(rangerService == null || provider == null) {
			throw new Exception("Provider "+provider+" not found");
		}
		return provider;
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
