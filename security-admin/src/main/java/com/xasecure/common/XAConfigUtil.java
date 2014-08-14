package com.xasecure.common;

import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

import com.xasecure.common.PropertiesUtil;

/**
 * 
 * 
 */
@Component
public class XAConfigUtil {
	static Logger logger = Logger.getLogger(XAConfigUtil.class);

	String webappRootURL;
	int defaultMaxRows = 250;
	String[] roles;
	boolean accessFilterEnabled = true;
	boolean isModerationEnabled = false;
	boolean isUserPrefEnabled = false;

	public XAConfigUtil() {
		webappRootURL = PropertiesUtil
				.getProperty("xa.webapp.url.root");
		if (webappRootURL == null || webappRootURL.trim().length() == 0) {
			logger.error("webapp URL is not set. Please xa.webapp.url.root property");
		}

		defaultMaxRows = PropertiesUtil.getIntProperty(
				"xa.db.maxrows.default", defaultMaxRows);

		roles = PropertiesUtil
				.getPropertyStringList("xa.users.roles.list");

		accessFilterEnabled = PropertiesUtil.getBooleanProperty(
				"xa.db.access.filter.enable", true);

		isModerationEnabled = PropertiesUtil.getBooleanProperty(
				"xa.moderation.enabled", isModerationEnabled);
		isUserPrefEnabled = PropertiesUtil.getBooleanProperty(
				"xa.userpref.enabled", isUserPrefEnabled);
	}	

	/**
	 * @return the defaultMaxRows
	 */
	public int getDefaultMaxRows() {
		return defaultMaxRows;
	}

	/**
	 * @return the roles
	 */
	public String[] getRoles() {
		return roles;
	}

	/**
	 * @return the accessFilterEnabled
	 */
	public boolean isAccessFilterEnabled() {
		return accessFilterEnabled;
	}

	/**
	 * @return the webAppRootURL
	 */
	public String getWebAppRootURL() {
		return webappRootURL;
	}

}
