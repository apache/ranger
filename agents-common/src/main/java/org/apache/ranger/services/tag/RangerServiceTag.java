package org.apache.ranger.services.tag;

import java.util.*;

import org.apache.commons.collections.MapUtils;
import org.apache.ranger.admin.client.RangerAdminRESTClient;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.service.RangerBaseService;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class RangerServiceTag extends RangerBaseService {

	private static final Log LOG = LogFactory.getLog(RangerServiceTag.class);

	public static final String TAG	= "tag";

	public RangerServiceTag() {
		super();
	}

	@Override
	public void init(RangerServiceDef serviceDef, RangerService service) {
		super.init(serviceDef, service);
	}

	@Override
	public HashMap<String,Object> validateConfig() throws Exception {
		HashMap<String, Object> ret = new HashMap<String, Object>();
		String 	serviceName  	    = getServiceName();
		boolean connectivityStatus = false;
		String message = null;

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerServiceTag.validateConfig -  Service: (" + serviceName + " )");
		}

		if (MapUtils.isEmpty(configs)) {
			message = "Configuration is null or empty";

		} else {
			String url = configs.get("URL");
			String sslConfigFileName = configs.get("SSL_CONFIG_FILE_NAME");
			String userName = configs.get("username");
			String password = configs.get("password");

			if (url == null || sslConfigFileName == null || userName == null || password == null) {
				message = "Either URL, SSL_CONFIG_FILE_NAME, username or password not provided in configuration";
			} else {

				RangerAdminRESTClient adminRESTClient = new RangerAdminRESTClient();
				adminRESTClient.init(serviceName, configs);

				try {
					adminRESTClient.getTagNames(null, ".*");	// Dont care about componentType
					connectivityStatus = true;
				} catch (Exception e) {
					LOG.error("RangerServiceTag.validateConfig() Error:" + e);
					connectivityStatus = false;
					message = "Cannot connect to TagResource Repository, " + e;
				}
			}
		}

		ret.put("connectivityStatus", connectivityStatus);
		ret.put("message", message);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerServiceTag.validateConfig - Response : (" + ret + " )");
		}

		return ret;
	}

	@Override
	public List<String> lookupResource(ResourceLookupContext context) throws Exception {
		String 	serviceName  	   = getServiceName();
		Map<String,String> configs = getConfigs();

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerServiceTag.lookupResource -  Context: (" + context + ")");
		}

		Set<String> tagNameSet = new HashSet<>();

		if (MapUtils.isNotEmpty(configs)) {
			String url = configs.get("URL");
			String sslConfigFileName = configs.get("SSL_CONFIG_FILE_NAME");
			String userName = configs.get("username");
			String password = configs.get("password");

			if (url != null && sslConfigFileName != null && userName != null && password != null) {

				if (context != null) {

					String userInput = context.getUserInput();
					String resource = context.getResourceName();
					Map<String, List<String>> resourceMap = context.getResources();
					final Set<String> userProvidedTagSet = new HashSet<String>();

					if (resource != null && resourceMap != null && resourceMap.get(TAG) != null) {

						for (String tag : resourceMap.get(TAG)) {
							userProvidedTagSet.add(tag);
						}

						try {
							String suffix = ".*";
							String tagNamePattern;

							if (userInput == null) {
								tagNamePattern = suffix;
							} else {
								tagNamePattern = userInput + suffix;
							}

							if(LOG.isDebugEnabled()) {
								LOG.debug("RangerServiceTag.lookupResource -  tagNamePattern : (" + tagNamePattern + ")");
							}

							RangerAdminRESTClient adminRESTClient = new RangerAdminRESTClient();
							adminRESTClient.init(serviceName, configs);

							tagNameSet = adminRESTClient.getTagNames(null, tagNamePattern); // Dont care about componentType

							tagNameSet.removeAll(userProvidedTagSet);

						} catch (Exception e) {
							LOG.error("RangerServiceTag.lookupResource -  Error : " + e);
						}
					}
				}

			}

		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerServiceTag.lookupResource()");
		}

		return new ArrayList<String>(tagNameSet);
	}
}
