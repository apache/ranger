package com.xasecure.usergroupsync;

import java.util.List;

public interface UserGroupSink {
	public void init() throws Throwable;

	public void addOrUpdateUser(String user, List<String> groups);
}
