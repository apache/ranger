package com.xasecure.authentication.unix.jaas;

import java.io.Serializable;
import java.security.Principal;

public class UnixGroupPrincipal implements Principal, Serializable {

	private static final long serialVersionUID = 8137147441841439754L;

	private String groupName ;
	
	public UnixGroupPrincipal(String groupName) {
		this.groupName = groupName ;
	}

	@Override
	public String getName() {
		return groupName ;
	}

}
