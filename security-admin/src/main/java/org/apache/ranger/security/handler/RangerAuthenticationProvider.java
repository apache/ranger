package org.apache.ranger.security.handler;

import java.security.Principal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;
import javax.security.auth.login.Configuration;

import org.apache.ranger.authentication.unix.jaas.RoleUserAuthorityGranter;
import org.apache.ranger.common.PropertiesUtil;
import org.springframework.ldap.core.support.LdapContextSource;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.authentication.jaas.DefaultJaasAuthenticationProvider;
import org.springframework.security.authentication.jaas.memory.InMemoryConfiguration;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.ldap.DefaultSpringSecurityContextSource;
import org.springframework.security.ldap.authentication.BindAuthenticator;
import org.springframework.security.ldap.authentication.LdapAuthenticationProvider;
import org.springframework.security.ldap.authentication.LdapAuthenticator;
import org.springframework.security.ldap.authentication.ad.ActiveDirectoryLdapAuthenticationProvider;
import org.springframework.security.ldap.userdetails.DefaultLdapAuthoritiesPopulator;



public class RangerAuthenticationProvider implements AuthenticationProvider {

	private String rangerAuthenticationMethod;

	private LdapAuthenticator authenticator;

	public RangerAuthenticationProvider() {

	}

	public Authentication initializeAuthenticationHandler(
			Authentication authentication) {
		if (rangerAuthenticationMethod.equalsIgnoreCase("LDAP")) {
			return getLdapAuthentication(authentication);
		}
		if (rangerAuthenticationMethod.equalsIgnoreCase("ACTIVE_DIRECTORY")
				|| rangerAuthenticationMethod.equalsIgnoreCase("AD")) {
			return getADAuthentication(authentication);
		}
		if (rangerAuthenticationMethod.equalsIgnoreCase("UNIX")) {
			return getUnixAuthentication(authentication);
		}

		return null;

	}

	private Authentication getLdapAuthentication(Authentication authentication) {

		try {
			// getting ldap settings
			String rangerLdapURL = PropertiesUtil.getProperty(
					"ranger.ldap.url", "");
			String rangerLdapUserDNPattern = PropertiesUtil.getProperty(
					"ranger.ldap.user.dnpattern", "");
			String rangerLdapGroupSearchBase = PropertiesUtil.getProperty(
					"ranger.ldap.group.searchbase", "");
			String rangerLdapGroupSearchFilter = PropertiesUtil.getProperty(
					"ranger.ldap.group.searchfilter", "");
			String rangerLdapGroupRoleAttribute = PropertiesUtil.getProperty(
					"ranger.ldap.group.roleattribute", "");
			String rangerLdapDefaultRole = PropertiesUtil.getProperty(
					"ranger.ldap.default.role", "");

			// taking the user-name and password from the authentication
			// object.
			String userName = authentication.getName();
			String userPassword = "";
			if (authentication.getCredentials() != null) {
				userPassword = authentication.getCredentials().toString();
			}

			// populating LDAP context source with LDAP URL and user-DN-pattern
			LdapContextSource ldapContextSource = new DefaultSpringSecurityContextSource(
					rangerLdapURL);

			ldapContextSource.setCacheEnvironmentProperties(false);
			ldapContextSource.setAnonymousReadOnly(true);

			// Creating LDAP authorities populator using Ldap context source and
			// Ldap group search base.
			// populating LDAP authorities populator with group search
			// base,group role attribute, group search filter.
			DefaultLdapAuthoritiesPopulator defaultLdapAuthoritiesPopulator = new DefaultLdapAuthoritiesPopulator(
					ldapContextSource, rangerLdapGroupSearchBase);
			defaultLdapAuthoritiesPopulator
					.setGroupRoleAttribute(rangerLdapGroupRoleAttribute);
			defaultLdapAuthoritiesPopulator
					.setGroupSearchFilter(rangerLdapGroupSearchFilter);
			defaultLdapAuthoritiesPopulator
					.setIgnorePartialResultException(true);

			// Creating BindAuthenticator using Ldap Context Source.
			BindAuthenticator bindAuthenticator = new BindAuthenticator(
					ldapContextSource);
			String[] userDnPatterns = new String[] { rangerLdapUserDNPattern };
			bindAuthenticator.setUserDnPatterns(userDnPatterns);

			// Creating Ldap authentication provider using BindAuthenticator and
			// Ldap authentication populator
			LdapAuthenticationProvider ldapAuthenticationProvider = new LdapAuthenticationProvider(
					bindAuthenticator, defaultLdapAuthoritiesPopulator);

			// getting user authenticated
			if (userName != null && userPassword != null
					&& !userName.trim().isEmpty()
					&& !userPassword.trim().isEmpty()) {
				final List<GrantedAuthority> grantedAuths = new ArrayList<>();
				grantedAuths.add(new SimpleGrantedAuthority(
						rangerLdapDefaultRole));

				final UserDetails principal = new User(userName, userPassword,
						grantedAuths);

				final Authentication finalAuthentication = new UsernamePasswordAuthenticationToken(
						principal, userPassword, grantedAuths);

				authentication = ldapAuthenticationProvider
						.authenticate(finalAuthentication);
				return authentication;
			} else {
				return null;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public Authentication getADAuthentication(Authentication authentication) {

		String rangerADURL = PropertiesUtil.getProperty("ranger.ldap.ad.url",
				"");
		String rangerADDomain = PropertiesUtil.getProperty(
				"ranger.ldap.ad.domain", "");
		String rangerLdapDefaultRole = PropertiesUtil.getProperty(
				"ranger.ldap.default.role", "");

		ActiveDirectoryLdapAuthenticationProvider adAuthenticationProvider = new ActiveDirectoryLdapAuthenticationProvider(
				rangerADDomain, rangerADURL);
		adAuthenticationProvider.setConvertSubErrorCodesToExceptions(true);
		adAuthenticationProvider.setUseAuthenticationRequestCredentials(true);

		// Grab the user-name and password out of the authentication object.
		String userName = authentication.getName();
		String userPassword = "";
		if (authentication.getCredentials() != null) {
			userPassword = authentication.getCredentials().toString();
		}

		// getting user authenticated
		if (userName != null && userPassword != null
				&& !userName.trim().isEmpty() && !userPassword.trim().isEmpty()) {
			final List<GrantedAuthority> grantedAuths = new ArrayList<>();
			grantedAuths.add(new SimpleGrantedAuthority(rangerLdapDefaultRole));
			final UserDetails principal = new User(userName, userPassword,
					grantedAuths);
			final Authentication finalAuthentication = new UsernamePasswordAuthenticationToken(
					principal, userPassword, grantedAuths);
			authentication = adAuthenticationProvider
					.authenticate(finalAuthentication);
			return authentication;
		} else {
			return null;
		}

	}

	public Authentication getUnixAuthentication(Authentication authentication) {

		try {
			String rangerLdapDefaultRole = PropertiesUtil.getProperty(
					"ranger.ldap.default.role", "");
			DefaultJaasAuthenticationProvider jaasAuthenticationProvider = new DefaultJaasAuthenticationProvider();
			String loginModuleName = "org.apache.ranger.authentication.unix.jaas.RemoteUnixLoginModule";
			LoginModuleControlFlag controlFlag = LoginModuleControlFlag.REQUIRED;
			Map<String, String> options = (Map<String, String>) new HashMap<String, String>();
			options.put("configFile", "ranger-admin-site.xml");
			AppConfigurationEntry appConfigurationEntry = new AppConfigurationEntry(
					loginModuleName, controlFlag, options);
			AppConfigurationEntry[] appConfigurationEntries = new AppConfigurationEntry[] { appConfigurationEntry };
			Map<String, AppConfigurationEntry[]> appConfigurationEntriesOptions = (Map<String, AppConfigurationEntry[]>) new HashMap<String, AppConfigurationEntry[]>();
			appConfigurationEntriesOptions.put("SPRINGSECURITY",
					appConfigurationEntries);
			Configuration configuration = new InMemoryConfiguration(
					appConfigurationEntriesOptions);

			jaasAuthenticationProvider.setConfiguration(configuration);

			RoleUserAuthorityGranter authorityGranter = new RoleUserAuthorityGranter();

			authorityGranter.grant((Principal) authentication.getPrincipal());

			RoleUserAuthorityGranter[] authorityGranters = new RoleUserAuthorityGranter[] { authorityGranter };

			jaasAuthenticationProvider.setAuthorityGranters(authorityGranters);

			String userName = authentication.getName();
			String userPassword = "";
			if (authentication.getCredentials() != null) {
				userPassword = authentication.getCredentials().toString();
			}

			// getting user authenticated
			if (userName != null && userPassword != null
					&& !userName.trim().isEmpty()
					&& !userPassword.trim().isEmpty()) {
				final List<GrantedAuthority> grantedAuths = new ArrayList<>();
				grantedAuths.add(new SimpleGrantedAuthority(
						rangerLdapDefaultRole));
				final UserDetails principal = new User(userName, userPassword,
						grantedAuths);
				final Authentication finalAuthentication = new UsernamePasswordAuthenticationToken(
						principal, userPassword, grantedAuths);
				authentication = jaasAuthenticationProvider
						.authenticate(finalAuthentication);
				return authentication;
			} else {
				return null;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return authentication;
	}

	@Override
	public Authentication authenticate(Authentication authentication)
			throws AuthenticationException {
		if (authentication != null) {
			return initializeAuthenticationHandler(authentication);
		}

		return null;
	}

	@Override
	public boolean supports(Class<?> authentication) {
		return authentication.equals(UsernamePasswordAuthenticationToken.class);
	}

	public String getRangerAuthenticationMethod() {
		return rangerAuthenticationMethod;
	}

	public void setRangerAuthenticationMethod(String rangerAuthenticationMethod) {
		this.rangerAuthenticationMethod = rangerAuthenticationMethod;
	}

	public LdapAuthenticator getAuthenticator() {
		return authenticator;
	}

	public void setAuthenticator(LdapAuthenticator authenticator) {
		this.authenticator = authenticator;
	}
}
