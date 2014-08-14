package com.xasecure.authorization.hive;

public class XaHiveAccessContext {
	private String mClientIpAddress;
	private String mClientType;
	private String mCommandString;
	private String mSessionString;
	
	public XaHiveAccessContext(String clientIpAddress, String clientType, String commandString, String sessionString) {
		mClientIpAddress = clientIpAddress;
		mClientType      = clientType;
		mCommandString   = commandString;
		mSessionString   = sessionString;
	}

	public String getClientIpAddress() {
		return mClientIpAddress;
	}

	public void setClientIpAddress(String clientIpAddress) {
		this.mClientIpAddress = clientIpAddress;
	}

	public String getClientType() {
		return mClientType;
	}

	public void setClientType(String clientType) {
		this.mClientType = clientType;
	}

	public String getCommandString() {
		return mCommandString;
	}

	public void setCommandString(String commandString) {
		this.mCommandString = commandString;
	}

	public String getSessionString() {
		return mSessionString;
	}

	public void setSessionString(String sessionString) {
		this.mSessionString = sessionString;
	}
}
