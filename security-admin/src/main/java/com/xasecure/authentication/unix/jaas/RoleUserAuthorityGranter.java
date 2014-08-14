package com.xasecure.authentication.unix.jaas;

import java.security.Principal;
import java.util.Collections;
import java.util.Set;

import org.springframework.security.authentication.jaas.AuthorityGranter;

public class RoleUserAuthorityGranter implements AuthorityGranter {

	@Override
	public Set<String> grant(Principal principal) {
		if (principal instanceof UnixGroupPrincipal) {
			Collections.singleton(principal.getName());
		}
		else {
			Collections.singleton("ROLE_USER");
		}
		return null;
	}
}
