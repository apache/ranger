/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.ranger.server.tomcat;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedAction;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.List;

import org.apache.catalina.Context;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.connector.Connector;
import org.apache.catalina.startup.Tomcat;
import org.apache.catalina.valves.AccessLogValve;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.security.SecureClientLogin;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.hadoop.security.alias.JavaKeyStoreProvider;
import org.apache.ranger.authorization.hadoop.utils.RangerCredentialProvider;
import org.apache.ranger.plugin.util.XMLUtils;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.security.auth.Subject;

public class EmbeddedServer {
	
	private static final Logger LOG = Logger.getLogger(EmbeddedServer.class
			.getName());
	private static final String DEFAULT_NAME_RULE = "DEFAULT";
	
	private static final String DEFAULT_CONFIG_FILENAME = "ranger-admin-default-site.xml";
	private static final String CORE_SITE_CONFIG_FILENAME = "core-site.xml";
	
	private static final String DEFAULT_WEBAPPS_ROOT_FOLDER = "webapps";
	
	private static String configFile = "ranger-admin-site.xml";
	
	private static final String AUTH_TYPE_KERBEROS = "kerberos";
    private static final String AUTHENTICATION_TYPE = "hadoop.security.authentication";
    private static final String ADMIN_USER_PRINCIPAL = "ranger.admin.kerberos.principal";
    private static final String SOLR_BOOTSTRAP_ENABLED = "ranger.audit.solr.bootstrap.enabled";
    private static final String ADMIN_USER_KEYTAB = "ranger.admin.kerberos.keytab";

	private static final String ADMIN_NAME_RULES = "hadoop.security.auth_to_local";
	private static final String ADMIN_SERVER_NAME = "rangeradmin";
	private static final String KMS_SERVER_NAME   = "rangerkms";
	
	private Properties serverConfigProperties = new Properties();
	public static final String RANGER_KEYSTORE_FILE_TYPE_DEFAULT = "jks";
	public static final String RANGER_TRUSTSTORE_FILE_TYPE_DEFAULT = "jks";
	public static final String RANGER_SSL_CONTEXT_ALGO_TYPE = "TLS";
	public static final String RANGER_SSL_KEYMANAGER_ALGO_TYPE = KeyManagerFactory.getDefaultAlgorithm();
	public static final String RANGER_SSL_TRUSTMANAGER_ALGO_TYPE = TrustManagerFactory.getDefaultAlgorithm();

	public static void main(String[] args) {
		new EmbeddedServer(args).start();
	}
	
	public EmbeddedServer(String[] args) {
		if (args.length > 0) {
			configFile = args[0];
		} else {
			XMLUtils.loadConfig(DEFAULT_CONFIG_FILENAME, serverConfigProperties);
		}
		XMLUtils.loadConfig(CORE_SITE_CONFIG_FILENAME, serverConfigProperties);
        XMLUtils.loadConfig(configFile, serverConfigProperties);
	}
	
	public static int DEFAULT_SHUTDOWN_PORT = 6185;
	public static String DEFAULT_SHUTDOWN_COMMAND = "SHUTDOWN";
	
	public void start() {
		SSLContext sslContext = getSSLContext();
		if (sslContext != null) {
			SSLContext.setDefault(sslContext);
		}
		final Tomcat server = new Tomcat();

		String logDir =  null;
		logDir = getConfig("logdir");
		if (logDir == null) {
			logDir = getConfig("kms.log.dir");
		}
		String servername = getConfig("servername");
		String hostName = getConfig("ranger.service.host");
		int serverPort = getIntConfig("ranger.service.http.port", 6181);
		int sslPort = getIntConfig("ranger.service.https.port", -1);
		int shutdownPort = getIntConfig("ranger.service.shutdown.port",DEFAULT_SHUTDOWN_PORT);
		String shutdownCommand = getConfig("ranger.service.shutdown.command",DEFAULT_SHUTDOWN_COMMAND);

		server.setHostname(hostName);
		server.setPort(serverPort);
		server.getServer().setPort(shutdownPort);
		server.getServer().setShutdown(shutdownCommand);

		boolean isHttpsEnabled = Boolean.valueOf(getConfig("ranger.service.https.attrib.ssl.enabled", "false"));
		boolean ajpEnabled = Boolean.valueOf(getConfig("ajp.enabled", "false"));

		if (ajpEnabled) {

			Connector ajpConnector = new Connector(
					"org.apache.coyote.ajp.AjpNioProtocol");
			ajpConnector.setPort(serverPort);
			ajpConnector.setProperty("protocol", "AJP/1.3");

			server.getService().addConnector(ajpConnector);

			// Making this as a default connector
			server.setConnector(ajpConnector);
			LOG.info("Created AJP Connector");
		} else if ((sslPort > 0) && isHttpsEnabled) {
			Connector ssl = new Connector();
			ssl.setPort(sslPort);
			ssl.setSecure(true);
			ssl.setScheme("https");
			ssl.setAttribute("SSLEnabled", "true");
			ssl.setAttribute("sslProtocol", getConfig("ranger.service.https.attrib.ssl.protocol", "TLS"));
			String clientAuth=getConfig("ranger.service.https.attrib.clientAuth", "false");
			if("false".equalsIgnoreCase(clientAuth)){
				clientAuth=getConfig("ranger.service.https.attrib.client.auth", "want");
			}
			ssl.setAttribute("clientAuth",clientAuth);
			String providerPath=getConfig("ranger.credential.provider.path");
			String keyAlias= getConfig("ranger.service.https.attrib.keystore.credential.alias","keyStoreCredentialAlias");
			String keystorePass=null;
			if(providerPath!=null && keyAlias!=null){
				keystorePass=getDecryptedString(providerPath.trim(), keyAlias.trim());
				if(keystorePass==null || keystorePass.trim().isEmpty() || "none".equalsIgnoreCase(keystorePass.trim())){
					keystorePass=getConfig("ranger.service.https.attrib.keystore.pass");
				}
			}
			ssl.setAttribute("keyAlias", getConfig("ranger.service.https.attrib.keystore.keyalias","rangeradmin"));
			ssl.setAttribute("keystorePass", keystorePass);
			ssl.setAttribute("keystoreFile", getKeystoreFile());
			
                        String defaultEnabledProtocols = "SSLv2Hello, TLSv1, TLSv1.1, TLSv1.2";
                        String enabledProtocols = getConfig("ranger.service.https.attrib.ssl.enabled.protocols", defaultEnabledProtocols);
			ssl.setAttribute("sslEnabledProtocols", enabledProtocols);
			String ciphers = getConfig("ranger.tomcat.ciphers");
			if (ciphers != null && ciphers.trim() != null && ciphers.trim().length() > 0) {
				ssl.setAttribute("ciphers", ciphers);
			}
			server.getService().addConnector(ssl);

			//
			// Making this as a default connector
			//
			server.setConnector(ssl);
			
		}
		updateHttpConnectorAttribConfig(server);
		
		File logDirectory = new File(logDir);
		if (!logDirectory.exists()) {
			logDirectory.mkdirs();
		}
		
		AccessLogValve valve = new AccessLogValve();
		valve.setRotatable(true);
		valve.setAsyncSupported(true);
		valve.setBuffered(false);
		valve.setEnabled(true);
		valve.setFileDateFormat(getConfig("ranger.accesslog.dateformat", "yyyy-MM-dd.HH"));
		valve.setDirectory(logDirectory.getAbsolutePath());
		valve.setSuffix(".log");

		String defaultAccessLogPattern = servername.equalsIgnoreCase(KMS_SERVER_NAME) ? "%h %l %u %t \"%m %U\" %s %b" : "%h %l %u %t \"%r\" %s %b";
		String logPattern = getConfig("ranger.accesslog.pattern", defaultAccessLogPattern);
		valve.setPattern(logPattern);	
				
		server.getHost().getPipeline().addValve(valve);
		
		try {
			String webapp_dir = getConfig("xa.webapp.dir");
			if (webapp_dir == null || webapp_dir.trim().isEmpty()) {
				// If webapp location property is not set, then let's derive
				// from catalina_base
				String catalina_base = getConfig("catalina.base");
				if (catalina_base == null || catalina_base.trim().isEmpty()) {
					LOG.severe("Tomcat Server failed to start: catalina.base and/or xa.webapp.dir is not set");
					System.exit(1);
				}
				webapp_dir = catalina_base + File.separator + "webapp";
				LOG.info("Deriving webapp folder from catalina.base property. folder="
						+ webapp_dir);
			}
			
			//String webContextName = getConfig("xa.webapp.contextName", "/");
			String webContextName = getConfig("ranger.contextName", "/");
			if (webContextName == null) {
				webContextName = "/";
			} else if (!webContextName.startsWith("/")) {
				LOG.info("Context Name [" + webContextName
						+ "] is being loaded as [ /" + webContextName + "]");
				webContextName = "/" + webContextName;
			}
			
			File wad = new File(webapp_dir);
			if (wad.isDirectory()) {
				LOG.info("Webapp file =" + webapp_dir + ", webAppName = "
						+ webContextName);
			} else if (wad.isFile()) {
				File webAppDir = new File(DEFAULT_WEBAPPS_ROOT_FOLDER);
				if (!webAppDir.exists()) {
					webAppDir.mkdirs();
				}
				LOG.info("Webapp file =" + webapp_dir + ", webAppName = "
						+ webContextName);
			}
			LOG.info("Adding webapp [" + webContextName + "] = path ["
					+ webapp_dir + "] .....");
			Context webappCtx = server.addWebapp(webContextName, new File(
					webapp_dir).getAbsolutePath());
			webappCtx.init();
			LOG.info("Finished init of webapp [" + webContextName
					+ "] = path [" + webapp_dir + "].");
		} catch (LifecycleException lce) {
			LOG.severe("Tomcat Server failed to start webapp:" + lce.toString());
			lce.printStackTrace();
		}
		
		if (servername.equalsIgnoreCase(ADMIN_SERVER_NAME)) {
			String keytab = getConfig(ADMIN_USER_KEYTAB);
			String principal = null;
			try {
				principal = SecureClientLogin.getPrincipal(getConfig(ADMIN_USER_PRINCIPAL), hostName);
			} catch (IOException ignored) {
				LOG.warning("Failed to get ranger.admin.kerberos.principal. Reason: " + ignored.toString());
			}
			String nameRules = getConfig(ADMIN_NAME_RULES);
			if (nameRules == null || nameRules.length() == 0) {
				LOG.info("Name is empty. Setting Name Rule as 'DEFAULT'");
				nameRules = DEFAULT_NAME_RULE;
			}
			if (getConfig(AUTHENTICATION_TYPE) != null
					&& getConfig(AUTHENTICATION_TYPE).trim().equalsIgnoreCase(AUTH_TYPE_KERBEROS)
					&& SecureClientLogin.isKerberosCredentialExists(principal,keytab)) {
				try{
					LOG.info("Provided Kerberos Credential : Principal = "
							+ principal + " and Keytab = " + keytab);
					Subject sub = SecureClientLogin.loginUserFromKeytab(principal, keytab, nameRules);
					Subject.doAs(sub, new PrivilegedAction<Void>() {
						@Override
						public Void run() {
							LOG.info("Starting Server using kerberos credential");
							startServer(server);
							return null;
						}
					});
				} catch (Exception e) {
					LOG.severe("Tomcat Server failed to start:" + e.toString());
					e.printStackTrace();
				}
			} else {
				startServer(server);
			}
		} else {
			startServer(server);
		}
	}

	private void startServer(final Tomcat server) {
		try {

			try {
				boolean solrBootstrapEnabled = Boolean.valueOf(getConfig(
						SOLR_BOOTSTRAP_ENABLED, "true"));
				if (solrBootstrapEnabled) {
					String servername = getConfig("servername");
					LOG.info("Server Name : " + servername);
					if (servername.equalsIgnoreCase(ADMIN_SERVER_NAME)) {
						SolrCollectionBootstrapper solrSetup = new SolrCollectionBootstrapper();
						solrSetup.start();
					}
				}
			} catch (Exception e) {
				LOG.severe("Error while setting solr " + e);
			}

			server.start();
			server.getServer().await();
			shutdownServer();
		} catch (LifecycleException e) {
			LOG.severe("Tomcat Server failed to start:" + e.toString());
			e.printStackTrace();
		} catch (Exception e) {
			LOG.severe("Tomcat Server failed to start:" + e.toString());
			e.printStackTrace();
		}
	}

	private String getKeystoreFile() {
		String keystoreFile=getConfig("ranger.service.https.attrib.keystore.file");
		if (keystoreFile == null || keystoreFile.trim().isEmpty()) {
			// new property not configured, lets use the old property
			keystoreFile = getConfig("ranger.https.attrib.keystore.file");
		}
		return keystoreFile;
	}

	protected String getConfig(String key) {
		String value = serverConfigProperties.getProperty(key);
		if (value == null || value.trim().isEmpty()) {
			// Value not found in properties file, let's try to get from
			// System's property
			value = System.getProperty(key);
		}
		return value;
	}
	
	protected String getConfig(String key, String defaultValue) {
		String ret = getConfig(key);
		if (ret == null) {
			ret = defaultValue;
		}
		return ret;
	}
	
	protected int getIntConfig(String key, int defaultValue) {
		int ret = defaultValue;
		String retStr = getConfig(key);
		try {
			if (retStr != null) {
				ret = Integer.parseInt(retStr);
			}
		} catch (Exception err) {
			LOG.warning(retStr + " can't be parsed to int. Reason: " + err.toString());
		}
		return ret;
	}
	
	public void shutdownServer() {
		int timeWaitForShutdownInSeconds = getIntConfig(
				"service.waitTimeForForceShutdownInSeconds", 0);
		if (timeWaitForShutdownInSeconds > 0) {
			long endTime = System.currentTimeMillis()
					+ (timeWaitForShutdownInSeconds * 1000L);
			LOG.info("Will wait for all threads to shutdown gracefully. Final shutdown Time: "
					+ new Date(endTime));
			while (System.currentTimeMillis() < endTime) {
				int activeCount = Thread.activeCount();
				if (activeCount == 0) {
				    LOG.info("Number of active threads = " + activeCount + ".");
					break;
				} else {
					LOG.info("Number of active threads = " + activeCount
							+ ". Waiting for all threads to shutdown ...");
					try {
						Thread.sleep(5000L);
					} catch (InterruptedException e) {
						LOG.warning("shutdownServer process is interrupted with exception: "
								+ e);
						break;
					}
				}
			}
		}
		LOG.info("Shuting down the Server.");
		System.exit(0);
	}

	protected long getLongConfig(String key, long defaultValue) {
		long ret = defaultValue;
		String retStr = getConfig(key);
		try{
			if (retStr != null) {
		        ret = Long.parseLong(retStr);
			}
		}catch(Exception err){
			LOG.warning(retStr + " can't be parsed to long. Reason: " + err.toString());
		}
		return ret;
	}
	public void updateHttpConnectorAttribConfig(Tomcat server) {
		server.getConnector().setAllowTrace(Boolean.valueOf(getConfig("ranger.service.http.connector.attrib.allowTrace","false")));
		server.getConnector().setAsyncTimeout(getLongConfig("ranger.service.http.connector.attrib.asyncTimeout", 10000));
		server.getConnector().setEnableLookups(Boolean.valueOf(getConfig("ranger.service.http.connector.attrib.enableLookups","false")));
		server.getConnector().setMaxHeaderCount(getIntConfig("ranger.service.http.connector.attrib.maxHeaderCount", 100));
		server.getConnector().setMaxParameterCount(getIntConfig("ranger.service.http.connector.attrib.maxParameterCount", 10000));
		server.getConnector().setMaxPostSize(getIntConfig("ranger.service.http.connector.attrib.maxPostSize", 2097152));
		server.getConnector().setMaxSavePostSize(getIntConfig("ranger.service.http.connector.attrib.maxSavePostSize", 4096));
		server.getConnector().setParseBodyMethods(getConfig("ranger.service.http.connector.attrib.methods", "POST"));
		server.getConnector().setURIEncoding(getConfig("ranger.service.http.connector.attrib.URIEncoding", "UTF-8"));
		server.getConnector().setXpoweredBy(false);
		server.getConnector().setAttribute("server", "Apache Ranger");
		Iterator<Object> iterator = serverConfigProperties.keySet().iterator();
		String key = null;
		String property = null;
		while (iterator.hasNext()){
			key = iterator.next().toString();
			if(key != null && key.startsWith("ranger.service.http.connector.property.")){
				property = key.replace("ranger.service.http.connector.property.","");
				server.getConnector().setProperty(property,getConfig(key));
				LOG.info(property + ":" + server.getConnector().getProperty(property));
			}
		}
	}

	public String getDecryptedString(String CrendentialProviderPath,String alias) {
		String credential=null;
		try{
			if(CrendentialProviderPath==null || alias==null||CrendentialProviderPath.trim().isEmpty()||alias.trim().isEmpty()){
				return null;
			}
			char[] pass = null;
			Configuration conf = new Configuration();
			String crendentialProviderPrefixJceks=JavaKeyStoreProvider.SCHEME_NAME + "://file";
			String crendentialProviderPrefixLocalJceks="localjceks://file";
			crendentialProviderPrefixJceks=crendentialProviderPrefixJceks.toLowerCase();
			CrendentialProviderPath=CrendentialProviderPath.trim();
			alias=alias.trim();
			if(CrendentialProviderPath.toLowerCase().startsWith(crendentialProviderPrefixJceks) ||  CrendentialProviderPath.toLowerCase().startsWith(crendentialProviderPrefixLocalJceks)){
				conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH,CrendentialProviderPath);
			}else{
				if(CrendentialProviderPath.startsWith("/")){
					conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH,JavaKeyStoreProvider.SCHEME_NAME + "://file" + CrendentialProviderPath);
				}else{
					conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH,JavaKeyStoreProvider.SCHEME_NAME + "://file/" + CrendentialProviderPath);
				}
			}
			List<CredentialProvider> providers = CredentialProviderFactory.getProviders(conf);
			List<String> aliasesList;
			CredentialProvider.CredentialEntry credEntry=null;
			for(CredentialProvider provider: providers) {
				//System.out.println("Credential Provider :" + provider);
				aliasesList=provider.getAliases();
				if(aliasesList!=null && aliasesList.contains(alias.toLowerCase())){
					credEntry=null;
					credEntry= provider.getCredentialEntry(alias);
					pass = credEntry.getCredential();
					if(pass!=null && pass.length>0){
						credential=String.valueOf(pass);
						break;
					}
				}
			}
		}catch(Exception ex){
			LOG.severe("CredentialReader failed while decrypting provided string. Reason: " + ex.toString());
			credential=null;
		}
		return credential;
	}

	private SSLContext getSSLContext() {
		KeyManager[] kmList = getKeyManagers();
		TrustManager[] tmList = getTrustManagers();
		SSLContext sslContext = null;
		if (tmList != null) {
			try {
				sslContext = SSLContext.getInstance(RANGER_SSL_CONTEXT_ALGO_TYPE);
				sslContext.init(kmList, tmList, new SecureRandom());
			} catch (NoSuchAlgorithmException e) {
				LOG.severe("SSL algorithm is not available in the environment. Reason: " + e.toString());
			} catch (KeyManagementException e) {
				LOG.severe("Unable to initials the SSLContext. Reason: " + e.toString());
			}
		}
		return sslContext;
	}

	private KeyManager[] getKeyManagers() {
		KeyManager[] kmList = null;
		String keyStoreFile = getConfig("ranger.keystore.file");
		String keyStoreAlias = getConfig("ranger.keystore.alias", "keyStoreCredentialAlias");
		if (StringUtils.isBlank(keyStoreFile)) {
			keyStoreFile = getKeystoreFile();
			keyStoreAlias = getConfig("ranger.service.https.attrib.keystore.credential.alias", "keyStoreCredentialAlias");
		}
		String credentialProviderPath = getConfig("ranger.credential.provider.path");
		String keyStoreFilepwd = getCredential(credentialProviderPath, keyStoreAlias);

		if (StringUtils.isNotEmpty(keyStoreFile) && StringUtils.isNotEmpty(keyStoreFilepwd)) {
			InputStream in = null;

			try {
				in = getFileInputStream(keyStoreFile);

				if (in != null) {
					KeyStore keyStore = KeyStore.getInstance(RANGER_KEYSTORE_FILE_TYPE_DEFAULT);

					keyStore.load(in, keyStoreFilepwd.toCharArray());

					KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(RANGER_SSL_KEYMANAGER_ALGO_TYPE);

					keyManagerFactory.init(keyStore, keyStoreFilepwd.toCharArray());

					kmList = keyManagerFactory.getKeyManagers();
				} else {
					LOG.severe("Unable to obtain keystore from file [" + keyStoreFile + "]");
				}
			} catch (KeyStoreException e) {
				LOG.log(Level.SEVERE, "Unable to obtain from KeyStore :" + e.getMessage(), e);
			} catch (NoSuchAlgorithmException e) {
				LOG.log(Level.SEVERE, "SSL algorithm is NOT available in the environment", e);
			} catch (CertificateException e) {
				LOG.log(Level.SEVERE, "Unable to obtain the requested certification ", e);
			} catch (FileNotFoundException e) {
				LOG.log(Level.SEVERE, "Unable to find the necessary SSL Keystore Files", e);
			} catch (IOException e) {
				LOG.log(Level.SEVERE, "Unable to read the necessary SSL Keystore Files", e);
			} catch (UnrecoverableKeyException e) {
				LOG.log(Level.SEVERE, "Unable to recover the key from keystore", e);
			} finally {
				close(in, keyStoreFile);
			}
		} else {
			if (StringUtils.isBlank(keyStoreFile)) {
				LOG.warning("Config 'ranger.keystore.file' or 'ranger.service.https.attrib.keystore.file' is not found or contains blank value");
			} else if (StringUtils.isBlank(keyStoreAlias)) {
				LOG.warning("Config 'ranger.keystore.alias' or 'ranger.service.https.attrib.keystore.credential.alias' is not found or contains blank value");
			} else if (StringUtils.isBlank(credentialProviderPath)) {
				LOG.warning("Config 'ranger.credential.provider.path' is not found or contains blank value");
			} else if (StringUtils.isBlank(keyStoreFilepwd)) {
				LOG.warning("Unable to read credential from credential store file ["+ credentialProviderPath + "] for given alias:"+keyStoreAlias);
			}
		}
		return kmList;
	}

	private TrustManager[] getTrustManagers() {
		TrustManager[] tmList = null;
		String truststoreFile = getConfig("ranger.truststore.file");
		String truststoreAlias = getConfig("ranger.truststore.alias");
		String credentialProviderPath = getConfig("ranger.credential.provider.path");
		String trustStoreFilepwd = getCredential(credentialProviderPath, truststoreAlias);

		if (StringUtils.isNotEmpty(truststoreFile) && StringUtils.isNotEmpty(trustStoreFilepwd)) {
			InputStream in = null;

			try {
				in = getFileInputStream(truststoreFile);

				if (in != null) {
					KeyStore trustStore = KeyStore.getInstance(RANGER_TRUSTSTORE_FILE_TYPE_DEFAULT);

					trustStore.load(in, trustStoreFilepwd.toCharArray());

					TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(RANGER_SSL_TRUSTMANAGER_ALGO_TYPE);

					trustManagerFactory.init(trustStore);

					tmList = trustManagerFactory.getTrustManagers();
				} else {
					LOG.log(Level.SEVERE, "Unable to obtain truststore from file [" + truststoreFile + "]");
				}
			} catch (KeyStoreException e) {
				LOG.log(Level.SEVERE, "Unable to obtain from KeyStore", e);
			} catch (NoSuchAlgorithmException e) {
				LOG.log(Level.SEVERE, "SSL algorithm is NOT available in the environment :" + e.getMessage(), e);
			} catch (CertificateException e) {
				LOG.log(Level.SEVERE, "Unable to obtain the requested certification :" + e.getMessage(), e);
			} catch (FileNotFoundException e) {
				LOG.log(Level.SEVERE, "Unable to find the necessary SSL TrustStore File:" + truststoreFile, e);
			} catch (IOException e) {
				LOG.log(Level.SEVERE, "Unable to read the necessary SSL TrustStore Files :" + truststoreFile, e);
			} finally {
				close(in, truststoreFile);
			}
		} else {
			if (StringUtils.isBlank(truststoreFile)) {
				LOG.warning("Config 'ranger.truststore.file' is not found or contains blank value!");
			} else if (StringUtils.isBlank(truststoreAlias)) {
				LOG.warning("Config 'ranger.truststore.alias' is not found or contains blank value!");
			} else if (StringUtils.isBlank(credentialProviderPath)) {
				LOG.warning("Config 'ranger.credential.provider.path' is not found or contains blank value!");
			} else if (StringUtils.isBlank(trustStoreFilepwd)) {
				LOG.warning("Unable to read credential from credential store file ["+ credentialProviderPath + "] for given alias:"+truststoreAlias);
			}
		}

		return tmList;
	}

	private String getCredential(String url, String alias) {
		return RangerCredentialProvider.getInstance().getCredentialString(url, alias);
	}

	private InputStream getFileInputStream(String fileName) throws IOException {
		InputStream in = null;
		if (StringUtils.isNotEmpty(fileName)) {
			File f = new File(fileName);
			if (f.exists()) {
				in = new FileInputStream(f);
			} else {
				in = ClassLoader.getSystemResourceAsStream(fileName);
			}
		}
		return in;
	}

	private void close(InputStream str, String filename) {
		if (str != null) {
			try {
				str.close();
			} catch (IOException excp) {
				LOG.log(Level.SEVERE, "Error while closing file: [" + filename + "]", excp);
			}
		}
	}

}
