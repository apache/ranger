package org.apache.ranger.services.tag;

import java.util.*;

import org.apache.commons.collections.MapUtils;
import org.apache.ranger.admin.client.RangerAdminClient;
import org.apache.ranger.admin.client.RangerAdminRESTClient;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.service.RangerBaseService;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.store.ServiceStore;
import org.apache.ranger.plugin.store.TagStore;
import org.apache.ranger.plugin.store.file.TagFileStore;

public class RangerServiceTag extends RangerBaseService {

	private static final Log LOG = LogFactory.getLog(RangerServiceTag.class);

	public static final String TAG	= "tag";

	public static final String propertyPrefix = "ranger.plugin.tag";

	public static final String applicationId = "Ranger-GUI";

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
		String 	serviceName         = getServiceName();
		boolean connectivityStatus  = false;
		String message              = null;

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerServiceTag.validateConfig -  Service: (" + serviceName + " )");
		}

		RangerAdminClient adminClient = createAdminClient(serviceName);

		try {
			adminClient.getTagNames(serviceName, null, ".*");	// Don't care about componentType
			connectivityStatus = true;
		} catch (Exception e) {
			LOG.error("RangerServiceTag.validateConfig() Error:" + e);
			connectivityStatus = false;
			message = "Cannot connect to TagResource Repository, Exception={" + e + "}. " + "Please check "
					+ propertyPrefix + " sub-properties.";
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

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerServiceTag.lookupResource -  Context: (" + context + ")");
		}

		List<String> tagNameList = new ArrayList<>();

		if (context != null) {

			String userInput = context.getUserInput();
			String resource = context.getResourceName();
			Map<String, List<String>> resourceMap = context.getResources();
			final List<String> userProvidedTagList = new ArrayList<>();

			if (resource != null && resourceMap != null && resourceMap.get(TAG) != null) {

				for (String tag : resourceMap.get(TAG)) {
					userProvidedTagList.add(tag);
				}

				String suffix = ".*";
				String tagNamePattern;

				if (userInput == null) {
					tagNamePattern = suffix;
				} else {
					tagNamePattern = userInput + suffix;
				}

				if (LOG.isDebugEnabled()) {
					LOG.debug("RangerServiceTag.lookupResource -  tagNamePattern : (" + tagNamePattern + ")");
				}

				try {

					RangerAdminClient adminClient = createAdminClient(serviceName);

					tagNameList = adminClient.getTagNames(serviceName, null, tagNamePattern); // Don't care about componentType

					tagNameList.removeAll(userProvidedTagList);

				} catch (Exception e) {
					LOG.error("RangerServiceTag.lookupResource -  Exception={" + e + "}. " + "Please check " +
							propertyPrefix + " sub-properties.");
				}
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerServiceTag.lookupResource()");
		}

		return tagNameList;
	}

	public static RangerAdminClient createAdminClient( String tagServiceName ) {
		return RangerBasePlugin.createAdminClient(tagServiceName, applicationId, propertyPrefix);
	}

}
