package org.apache.ranger.security.web.filter;

import java.security.interfaces.RSAPublicKey;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SSOAuthenticationProperties {

	  private String authenticationProviderUrl = null;
	  private RSAPublicKey publicKey = null;
	  private String cookieName = "hadoop-jwt";
	  private String originalUrlQueryParam = null;
	  private String[] userAgentList = null; 

	  public String getAuthenticationProviderUrl() {
	    return authenticationProviderUrl;
	  }

	  public void setAuthenticationProviderUrl(String authenticationProviderUrl) {
	    this.authenticationProviderUrl = authenticationProviderUrl;
	  }

	  public RSAPublicKey getPublicKey() {
	    return publicKey;
	  }

	  public void setPublicKey(RSAPublicKey publicKey) {
	    this.publicKey = publicKey;
	  }

	  public String getCookieName() {
	    return cookieName;
	  }

	  public void setCookieName(String cookieName) {
	    this.cookieName = cookieName;
	  }

	  public String getOriginalUrlQueryParam() {
	    return originalUrlQueryParam;
	  }

	  public void setOriginalUrlQueryParam(String originalUrlQueryParam) {
	    this.originalUrlQueryParam = originalUrlQueryParam;
	  }

	/**
	 * @return the userAgentList
	 */
	public String[] getUserAgentList() {
		return userAgentList;
	}

	/**
	 * @param userAgentList the userAgentList to set
	 */
	public void setUserAgentList(String[] userAgentList) {
		this.userAgentList = userAgentList;
	}
}

