package org.apache.ranger.security.web.filter;

import com.nimbusds.jwt.SignedJWT;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;

import java.util.Collection;

/**
 * Internal token which describes JWT authentication
 */
public class SSOAuthentication implements Authentication {

  private SignedJWT token;
  private boolean authenticated = false;

  public SSOAuthentication(SignedJWT token) {
    this.token = token;
  }

  @Override
  public SignedJWT getCredentials() {
    return token;
  }

  @Override
  public Object getDetails() {
    return null;
  }

  @Override
  public boolean isAuthenticated() {
    return authenticated;
  }

  @Override
  public void setAuthenticated(boolean authenticated) throws IllegalArgumentException {
    this.authenticated = authenticated;
  }

  @Override
  public String getName() {	
	  return null;
  }

  @Override
  public Collection<? extends GrantedAuthority> getAuthorities() {
	  return null;
  }

  @Override
  public Object getPrincipal() {
	  return null;
  }  
}