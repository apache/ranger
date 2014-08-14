package com.xasecure.usergroupsync;


public interface UserGroupSource {
	public void init() throws Throwable;

	public boolean isChanged();

	public void updateSink(UserGroupSink sink) throws Throwable;
}
