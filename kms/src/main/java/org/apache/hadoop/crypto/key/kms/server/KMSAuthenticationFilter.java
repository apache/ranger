/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.crypto.key.kms.server;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.kms.KMSDelegationToken;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticationFilter;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticationHandler;
import org.apache.hadoop.security.token.delegation.web.KerberosDelegationTokenAuthenticationHandler;
import org.apache.hadoop.security.token.delegation.web.PseudoDelegationTokenAuthenticationHandler;

import com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.http.HtmlQuoting;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * Authentication filter that takes the configuration from the KMS configuration
 * file.
 */
@InterfaceAudience.Private
public class KMSAuthenticationFilter
    extends DelegationTokenAuthenticationFilter {

  public static final String CONFIG_PREFIX = KMSConfiguration.CONFIG_PREFIX +
      "authentication.";
  static final String RANGER_KMS_REST_API_PATH = "/kms/api/status";

  @Override
  protected Properties getConfiguration(String configPrefix,
      FilterConfig filterConfig) {
    Configuration conf = KMSWebApp.getConfiguration();
    return this.getKMSConfiguration(conf);
  }

  @VisibleForTesting
  Properties getKMSConfiguration(Configuration conf) {
	Properties props = new Properties();
	Map<String, String> propsWithPrefixMap = conf.getPropsWithPrefix(CONFIG_PREFIX);

	for (Map.Entry<String, String> entry : propsWithPrefixMap.entrySet()) {
		props.setProperty(entry.getKey(), entry.getValue());
	}

    String authType = props.getProperty(AUTH_TYPE,"simple");
    if (authType.equals(PseudoAuthenticationHandler.TYPE)) {
      props.setProperty(AUTH_TYPE,
          PseudoDelegationTokenAuthenticationHandler.class.getName());
    } else if (authType.equals(KerberosAuthenticationHandler.TYPE)) {
      props.setProperty(AUTH_TYPE,
          KerberosDelegationTokenAuthenticationHandler.class.getName());
    }
    props.setProperty(DelegationTokenAuthenticationHandler.TOKEN_KIND,
        KMSDelegationToken.TOKEN_KIND.toString());
    return props;
  }

  protected Configuration getProxyuserConfiguration(FilterConfig filterConfig) {
    Map<String, String> proxyuserConf = KMSWebApp.getConfiguration().
        getValByRegex("hadoop\\.kms\\.proxyuser\\.");
    Configuration conf = new Configuration(false);
    for (Map.Entry<String, String> entry : proxyuserConf.entrySet()) {
      conf.set(entry.getKey().substring("hadoop.kms.".length()),
          entry.getValue());
    }
    return conf;
  }

  private static class KMSResponse extends HttpServletResponseWrapper {
    public int statusCode;
    public String msg;

    public KMSResponse(ServletResponse response) {
      super((HttpServletResponse)response);
    }

    @Override
    public void setStatus(int sc) {
      statusCode = sc;
      super.setStatus(sc);
    }

    @Override
    public void sendError(int sc, String msg) throws IOException {
      statusCode = sc;
      this.msg = msg;
      super.sendError(sc, HtmlQuoting.quoteHtmlChars(msg));
    }

    @Override
    public void sendError(int sc) throws IOException {
      statusCode = sc;
      super.sendError(sc);
    }

    @Override
    public void setStatus(int sc, String sm) {
      statusCode = sc;
      msg = sm;
      super.setStatus(sc, sm);
    }
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response,
      FilterChain filterChain) throws IOException, ServletException {
    KMSResponse kmsResponse = new KMSResponse(response);
    String path = ((HttpServletRequest) request).getRequestURI();
    	if (path.startsWith(RANGER_KMS_REST_API_PATH)) {
    		filterChain.doFilter(request, response);
		} else {
			super.doFilter(request, kmsResponse, filterChain);

			if (kmsResponse.statusCode != HttpServletResponse.SC_OK
					&& kmsResponse.statusCode != HttpServletResponse.SC_CREATED
					&& kmsResponse.statusCode != HttpServletResponse.SC_UNAUTHORIZED) {
				KMSWebApp.getInvalidCallsMeter().mark();
			}

			// HttpServletResponse.SC_UNAUTHORIZED is because the request does not
			// belong to an authenticated user.
			if (kmsResponse.statusCode == HttpServletResponse.SC_UNAUTHORIZED) {

				KMSWebApp.getUnauthenticatedCallsMeter().mark();
				String method = ((HttpServletRequest) request).getMethod();
				StringBuffer requestURL = ((HttpServletRequest) request).getRequestURL();
				String queryString = ((HttpServletRequest) request).getQueryString();
				if (queryString != null) {
					requestURL.append("?").append(queryString);
				}

				if (!method.equals("OPTIONS")) {
					// an HTTP OPTIONS request is made as part of the SPNEGO authentication
					// sequence. We do not need to audit log it, since it doesn't belong
					// to KMS context. KMS server doesn't handle OPTIONS either.
					KMSWebApp.getKMSAudit().unauthenticated(request.getRemoteHost(), method, requestURL.toString(),
							kmsResponse.msg);
				}
			}
		}
  }
}
