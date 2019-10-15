/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.crypto.key;

import com.microsoft.azure.keyvault.KeyVaultClient;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.security.Key;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.crypto.KeyGenerator;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.ranger.plugin.util.JsonUtilsV2;
import org.apache.hadoop.fs.Path;
import org.apache.ranger.credentialapi.CredentialReader;
import org.apache.ranger.kms.dao.DaoManager;
import org.apache.log4j.Logger;

@InterfaceAudience.Private
public class RangerKeyStoreProvider extends KeyProvider {

	static final Logger logger = Logger.getLogger(RangerKeyStoreProvider.class);

	public static final String SCHEME_NAME = "dbks";
	public static final String KMS_CONFIG_DIR = "kms.config.dir";
	public static final String DBKS_SITE_XML = "dbks-site.xml";
	public static final String ENCRYPTION_KEY = "ranger.db.encrypt.key.password";
	private static final String KEY_METADATA = "KeyMetadata";
	private static final String CREDENTIAL_PATH = "ranger.ks.jpa.jdbc.credential.provider.path";
	private static final String MK_CREDENTIAL_ALIAS = "ranger.ks.masterkey.credential.alias";
	private static final String DB_CREDENTIAL_ALIAS = "ranger.ks.jpa.jdbc.credential.alias";
	private static final String DB_PASSWORD = "ranger.ks.jpa.jdbc.password";
	private static final String HSM_ENABLED = "ranger.ks.hsm.enabled";
	private static final String HSM_PARTITION_PASSWORD_ALIAS = "ranger.ks.hsm.partition.password.alias";
	private static final String HSM_PARTITION_PASSWORD = "ranger.ks.hsm.partition.password";
	private static final String KEYSECURE_ENABLED = "ranger.kms.keysecure.enabled";
	private static final String KEYSECURE_USERNAME = "ranger.kms.keysecure.login.username";
	private static final String KEYSECURE_PASSWORD_ALIAS = "ranger.kms.keysecure.login.password.alias";
	private static final String KEYSECURE_PASSWORD = "ranger.kms.keysecure.login.password";
	private static final String KEYSECURE_LOGIN = "ranger.kms.keysecure.login";
	private static final String AZURE_KEYVAULT_ENABLED = "ranger.kms.azurekeyvault.enabled";
	private static final String AZURE_KEYVAULT_SSL_ENABLED = "ranger.kms.azure.keyvault.ssl.enabled";
	private static final String AZURE_CLIENT_ID = "ranger.kms.azure.client.id";
	private static final String AZURE_CLIENT_SECRET_ALIAS = "ranger.kms.azure.client.secret.alias";
	private static final String AZURE_CLIENT_SECRET = "ranger.kms.azure.client.secret";
	private static final String AZURE_KEYVAULT_CERTIFICATE_PATH = "ranger.kms.azure.keyvault.certificate.path";
	private static final String AZURE_KEYVAULT_CERTIFICATE_PASSWORD = "ranger.kms.azure.keyvault.certificate.password";
	private final RangerKeyStore dbStore;
	private char[] masterKey;
	private boolean changed = false;
	private final Map<String, Metadata> cache = new HashMap<String, Metadata>();
	private DaoManager daoManager;
	private Lock readLock;
	private boolean azureKeyVaultEnabled = false;

	public RangerKeyStoreProvider(Configuration conf) throws Throwable {
		super(conf);
		if (logger.isDebugEnabled()) {
			logger.debug("==> RangerKeyStoreProvider.Configuration(conf)");
		}
		conf = getDBKSConf();
		getFromJceks(conf, CREDENTIAL_PATH, MK_CREDENTIAL_ALIAS, ENCRYPTION_KEY);
		getFromJceks(conf, CREDENTIAL_PATH, DB_CREDENTIAL_ALIAS, DB_PASSWORD);
		getFromJceks(conf, CREDENTIAL_PATH, HSM_PARTITION_PASSWORD_ALIAS,
				HSM_PARTITION_PASSWORD);
		RangerKMSDB rangerKMSDB = new RangerKMSDB(conf);
		daoManager = rangerKMSDB.getDaoManager();

		RangerKMSMKI rangerMasterKey = null;
		String password = conf.get(ENCRYPTION_KEY);
		if (password == null || password.trim().equals("")
				|| password.trim().equals("_")
				|| password.trim().equals("crypted")) {
			throw new IOException(
					"The Ranger MasterKey Password is empty or not a valid Password");
		}
		if (StringUtils.isEmpty(conf.get(HSM_ENABLED))
				|| conf.get(HSM_ENABLED).equalsIgnoreCase("false")) {
			logger.info("Ranger KMS Database is enabled for storing master key.");
			rangerMasterKey = new RangerMasterKey(daoManager);
		} else {
			logger.info("Ranger KMS HSM is enabled for storing master key.");
			rangerMasterKey = new RangerHSM(conf);
			String partitionPasswd = conf.get(HSM_PARTITION_PASSWORD);
			if (partitionPasswd == null || partitionPasswd.trim().equals("")
					|| partitionPasswd.trim().equals("_")
					|| partitionPasswd.trim().equals("crypted")) {
				throw new IOException("Partition Password doesn't exists");
			}
		}

		if (conf != null && StringUtils.isNotEmpty(conf.get(KEYSECURE_ENABLED))
				&& conf.get(KEYSECURE_ENABLED).equalsIgnoreCase("true")) {
			getFromJceks(conf, CREDENTIAL_PATH, KEYSECURE_PASSWORD_ALIAS,
					KEYSECURE_PASSWORD);
			String keySecureLoginCred = conf.get(KEYSECURE_USERNAME).trim()
					+ ":" + conf.get(KEYSECURE_PASSWORD);
			conf.set(KEYSECURE_LOGIN, keySecureLoginCred);

			rangerMasterKey = new RangerSafenetKeySecure(conf);

			dbStore = new RangerKeyStore(daoManager);
			// generate master key on key secure server
			rangerMasterKey.generateMasterKey(password);
			try {
				masterKey = rangerMasterKey.getMasterKey(password)
						.toCharArray();
			} catch (Exception ex) {
				throw new Exception(
						"Error while getting Safenet KeySecure master key "
								+ ex);
			}

		} else if (conf != null
				&& StringUtils.isNotEmpty(conf.get(AZURE_KEYVAULT_ENABLED))
				&& conf.get(AZURE_KEYVAULT_ENABLED).equalsIgnoreCase("true")) {
			azureKeyVaultEnabled = true;
			getFromJceks(conf, CREDENTIAL_PATH, AZURE_CLIENT_SECRET_ALIAS,
					AZURE_CLIENT_SECRET);
			String azureClientId = conf.get(AZURE_CLIENT_ID);
			if (StringUtils.isEmpty(azureClientId)) {
				throw new Exception(
						"Azure Key Vault is enabled and client id is not configured");
			}
			String azureClientSecret = conf.get(AZURE_CLIENT_SECRET);
			AzureKeyVaultClientAuthenticator azureKVClientAuthenticator;
			KeyVaultClient kvClient = null;
			if (conf != null
					&& StringUtils.isNotEmpty(conf.get(AZURE_KEYVAULT_SSL_ENABLED))
					&& conf.get(AZURE_KEYVAULT_SSL_ENABLED).equalsIgnoreCase("false")) {
				try {
					if (StringUtils.isEmpty(azureClientSecret)) {
						throw new Exception(
								"Azure Key Vault is enabled in non SSL mode and client password/secret is not configured");
					}
					azureKVClientAuthenticator = new AzureKeyVaultClientAuthenticator(
							azureClientId, azureClientSecret);
					kvClient = new KeyVaultClient(azureKVClientAuthenticator);
				} catch (Exception ex) {
					throw new Exception(
							"Error while getting key vault client object with client id and client secret : "
									+ ex);
				}
			} else {
				try {
					azureKVClientAuthenticator = new AzureKeyVaultClientAuthenticator(
							azureClientId);
					String keyVaultCertPath = conf
							.get(AZURE_KEYVAULT_CERTIFICATE_PATH);
					if (StringUtils.isEmpty(keyVaultCertPath)) {
						throw new Exception(
								"Azure Key Vault is enabled in SSL mode. Please provide certificate path for authentication.");
					}
					String keyVaultCertPassword = conf
							.get(AZURE_KEYVAULT_CERTIFICATE_PASSWORD);

					kvClient = !StringUtils.isEmpty(keyVaultCertPassword) ? azureKVClientAuthenticator
							.getAuthentication(keyVaultCertPath,
									keyVaultCertPassword)
							: azureKVClientAuthenticator.getAuthentication(
									keyVaultCertPath, "");
				} catch (Exception ex) {
					throw new Exception(
							"Error while getting key vault client object with client id and certificate. Error :  : "
									+ ex);
				}
			}
			boolean success = false;
			if (kvClient != null) {
				try {
					dbStore = new RangerKeyStore(daoManager, conf, kvClient);
					rangerMasterKey = new RangerKeyVaultKeyGenerator(conf,
							kvClient);
					if (rangerMasterKey != null) {
						success = rangerMasterKey.generateMasterKey(password);
					}
				} catch (Exception ex) {
					throw new Exception(
							"Error while generating master key and master key secret in Azure key vault. Error :  : "
									+ ex);
				}
			} else {
				throw new Exception(
						"Unable to get Key Vault Client. Please check the azure credentials.");
			}
			if (success) {
				try {
					/* Master key not exportable from key vault */
					masterKey = null;
				} catch (Exception ex) {
					throw new Exception(
							"Error while getting Azure Master key Secret. Error : "
									+ ex);
				}
			}

		} else {
			dbStore = new RangerKeyStore(daoManager);
			rangerMasterKey.generateMasterKey(password);
			// code to retrieve rangerMasterKey password
			try {
				masterKey = rangerMasterKey.getMasterKey(password)
						.toCharArray();
			} catch (Exception ex) {
				throw new Exception("Error while getting Ranger Master key "
						+ ex);
			}
		}
		reloadKeys();
		ReadWriteLock lock = new ReentrantReadWriteLock(true);
		readLock = lock.readLock();
	}

	public static Configuration getDBKSConf() {
		Configuration newConfig = getConfiguration(true, DBKS_SITE_XML);
		getFromJceks(newConfig, CREDENTIAL_PATH, MK_CREDENTIAL_ALIAS,
				ENCRYPTION_KEY);
		getFromJceks(newConfig, CREDENTIAL_PATH, DB_CREDENTIAL_ALIAS,
				DB_PASSWORD);
		return newConfig;

	}

	static Configuration getConfiguration(boolean loadHadoopDefaults,
			String... resources) {
		Configuration conf = new Configuration(loadHadoopDefaults);
		String confDir = System.getProperty(KMS_CONFIG_DIR);
		if (confDir != null) {
			try {
				Path confPath = new Path(confDir);
				if (!confPath.isUriPathAbsolute()) {
					throw new RuntimeException("System property '"
							+ KMS_CONFIG_DIR + "' must be an absolute path: "
							+ confDir);
				}
				for (String resource : resources) {
					conf.addResource(new URL("file://"
							+ new Path(confDir, resource).toUri()));
				}
			} catch (MalformedURLException ex) {
				logger.error(
						"==> RangerKeyStoreProvider.getConfiguration() error : ",
						ex);
				throw new RuntimeException(ex);
			}
		} else {
			for (String resource : resources) {
				conf.addResource(resource);
			}
		}
		return conf;
	}

	private void loadKeys(char[] masterKey) throws NoSuchAlgorithmException,
			CertificateException, IOException {
		if (logger.isDebugEnabled()) {
			logger.debug("==> RangerKeyStoreProvider.loadKeys()");
		}
		dbStore.engineLoad(null, masterKey);
	}

	@Override
	public KeyVersion createKey(String name, byte[] material, Options options)
			throws IOException {
		if (logger.isDebugEnabled()) {
			logger.debug("==> RangerKeyStoreProvider.createKey()");
		}
		reloadKeys();
		if (dbStore.engineContainsAlias(name) || cache.containsKey(name)) {
			throw new IOException("Key " + name + " already exists");
		}

		if (dbStore.engineContainsAlias(name) || cache.containsKey(name)) {
			throw new IOException("Key " + name + " already exists");
		}
		Metadata meta = new Metadata(options.getCipher(),
				options.getBitLength(), options.getDescription(),
				options.getAttributes(), new Date(), 1);

		if (options.getBitLength() != 8 * material.length) {
			throw new IOException("Wrong key length. Required "
					+ options.getBitLength() + ", but got "
					+ (8 * material.length));
		}
		cache.put(name, meta);
		String versionName = buildVersionName(name, 0);
		if (logger.isDebugEnabled()) {
			logger.debug("<== RangerKeyStoreProvider.createKey()");
		}
		return innerSetKeyVersion(name, versionName, material,
				meta.getCipher(), meta.getBitLength(), meta.getDescription(),
				meta.getVersions(), meta.getAttributes());
	}

	KeyVersion innerSetKeyVersion(String name, String versionName,
			byte[] material, String cipher, int bitLength, String description,
			int version, Map<String, String> attributes) throws IOException {
		if (logger.isDebugEnabled()) {
			logger.debug("==> RangerKeyStoreProvider.innerSetKeyVersion()");
			logger.debug("name : " + name + " and versionName : " + versionName);
		}
		try {
			String attribute = JsonUtilsV2.mapToJson(attributes);
			if (azureKeyVaultEnabled) {
				dbStore.addSecureKeyByteEntry(versionName, new SecretKeySpec(
						material, cipher), cipher, bitLength, description,
						version, attribute);
			} else {
				dbStore.addKeyEntry(versionName, new SecretKeySpec(material,
						cipher), masterKey, cipher, bitLength, description,
						version, attribute);
			}
		} catch (Exception e) {
			throw new IOException("Can't store key " + versionName, e);
		}
		changed = true;
		if (logger.isDebugEnabled()) {
			logger.debug("<== RangerKeyStoreProvider.innerSetKeyVersion()");
		}
		return new KeyVersion(name, versionName, material);
	}

	@Override
	public void deleteKey(String name) throws IOException {
		if (logger.isDebugEnabled()) {
			logger.debug("==> RangerKeyStoreProvider.deleteKey(" + name + ")");
		}
		reloadKeys();
		Metadata meta = getMetadata(name);
		if (meta == null) {
			throw new IOException("Key " + name + " does not exist");
		}
		for (int v = 0; v < meta.getVersions(); ++v) {
			String versionName = buildVersionName(name, v);
			try {
				if (dbStore.engineContainsAlias(versionName)) {
					dbStore.engineDeleteEntry(versionName);
				}
			} catch (KeyStoreException e) {
				throw new IOException("Problem removing " + versionName, e);
			}
		}
		try {
			if (dbStore.engineContainsAlias(name)) {
				dbStore.engineDeleteEntry(name);
			}
		} catch (KeyStoreException e) {
			throw new IOException("Problem removing " + name + " from " + this,
					e);
		}
		cache.remove(name);
		changed = true;
	}

	@Override
	public void flush() throws IOException {
		try {
			if (!changed) {
				return;
			}
			// put all of the updates into the db
			for (Map.Entry<String, Metadata> entry : cache.entrySet()) {
				try {
					Metadata metadata = entry.getValue();
					String attributes = JsonUtilsV2.mapToJson(metadata
							.getAttributes());
					if (azureKeyVaultEnabled) {
						Key ezkey = new KeyMetadata(metadata);
						if (ezkey.getEncoded().length == 0) {
							KeyGenerator keyGenerator = KeyGenerator
									.getInstance(metadata.getAlgorithm());
							keyGenerator.init(metadata.getBitLength());
							byte[] key = keyGenerator.generateKey()
									.getEncoded();
							ezkey = new SecretKeySpec(key, metadata.getCipher());
						}

						dbStore.addSecureKeyByteEntry(entry.getKey(), ezkey,
								metadata.getCipher(), metadata.getBitLength(),
								metadata.getDescription(),
								metadata.getVersions(), attributes);
					} else {
						dbStore.addKeyEntry(entry.getKey(), new KeyMetadata(
								metadata), masterKey, metadata.getAlgorithm(),
								metadata.getBitLength(), metadata
										.getDescription(), metadata
										.getVersions(), attributes);
					}
				} catch (Exception e) {
					throw new IOException("Can't set metadata key "
							+ entry.getKey(), e);
				}
			}
			try {
				dbStore.engineStore(null, masterKey);
				reloadKeys();
			} catch (NoSuchAlgorithmException e) {
				throw new IOException("No such algorithm storing key", e);
			} catch (CertificateException e) {
				throw new IOException("Certificate exception storing key", e);
			}
			changed = false;
		} catch (IOException ioe) {
			cache.clear();
			reloadKeys();
			throw ioe;
		}
	}

	@Override
	public KeyVersion getKeyVersion(String versionName) throws IOException {
		readLock.lock();

		try {
			if (azureKeyVaultEnabled) {
				byte[] decryptKeyByte = null;
				try {
					if (!dbStore.engineContainsAlias(versionName)) {
						dbStore.engineLoad(null, masterKey);
						if (!dbStore.engineContainsAlias(versionName)) {
							return null;
						}
					}
					try {
						decryptKeyByte = dbStore
								.engineGetDecryptedZoneKeyByte(versionName);
					} catch (Exception e) {
						throw new RuntimeException(
								"Error while getting decrypted key." + e);
					}
					if (decryptKeyByte == null || decryptKeyByte.length == 0) {
						return null;
					} else {
						return new KeyVersion(getBaseName(versionName),
								versionName, decryptKeyByte);
					}

				} catch (NoSuchAlgorithmException e) {
					throw new IOException("Can't get algorithm for key "
							+ e.getMessage());
				} catch (CertificateException e) {

					throw new IOException("Certificate exception storing key",
							e);
				}
			} else {
				SecretKeySpec key = null;
				try {
					if (!dbStore.engineContainsAlias(versionName)) {
						dbStore.engineLoad(null, masterKey);
						if (!dbStore.engineContainsAlias(versionName)) {
							return null;
						}
					}
					key = (SecretKeySpec) dbStore.engineGetKey(versionName,
							masterKey);
				} catch (NoSuchAlgorithmException e) {
					throw new IOException("Can't get algorithm for key " + key,
							e);
				} catch (UnrecoverableKeyException e) {
					throw new IOException("Can't recover key " + key, e);
				} catch (CertificateException e) {
					throw new IOException("Certificate exception storing key",
							e);
				}
				if (key == null) {
					return null;
				} else {
					return new KeyVersion(getBaseName(versionName),
							versionName, key.getEncoded());
				}
			}
		} finally {
			readLock.unlock();
		}
	}

	@Override
	public List<KeyVersion> getKeyVersions(String name) throws IOException {
		List<KeyVersion> list = new ArrayList<KeyVersion>();
		Metadata km = getMetadata(name);
		if (km != null) {
			int latestVersion = km.getVersions();
			KeyVersion v = null;
			String versionName = null;
			for (int i = 0; i < latestVersion; i++) {
				versionName = buildVersionName(name, i);
				v = getKeyVersion(versionName);
				if (v != null) {
					list.add(v);
				}
			}
		}
		return list;
	}

	@Override
	public List<String> getKeys() throws IOException {
		ArrayList<String> list = new ArrayList<String>();
		String alias = null;
		reloadKeys();
		Enumeration<String> e = dbStore.engineAliases();
		while (e.hasMoreElements()) {
			alias = e.nextElement();
			// only include the metadata key names in the list of names
			if (!alias.contains("@")) {
				list.add(alias);
			}
		}
		return list;
	}

	@Override
	public Metadata getMetadata(String name) throws IOException {
		try {
			readLock.lock();
			if (cache.containsKey(name)) {
				Metadata meta = cache.get(name);
				return meta;
			}
			try {
				if (!dbStore.engineContainsAlias(name)) {
					dbStore.engineLoad(null, masterKey);
					if (!dbStore.engineContainsAlias(name)) {
						return null;
					}
				}
				if (azureKeyVaultEnabled) {
					Metadata meta = dbStore.engineGetKeyMetadata(name);
					if (meta != null) {
						cache.put(name, meta);
						return meta;
					}
				} else {
					Key key = dbStore.engineGetKey(name, masterKey);
					if (key != null) {
						Metadata meta = ((KeyMetadata) key).metadata;
						cache.put(name, meta);
						return meta;
					}
				}
			} catch (NoSuchAlgorithmException e) {
				throw new IOException("Can't get algorithm for " + name, e);
			} catch (UnrecoverableKeyException e) {
				throw new IOException("Can't recover key for " + name, e);
			}
			return null;
		} catch (Exception e) {
			throw new IOException("Please try again ", e);
		} finally {
			readLock.unlock();
		}
	}

	@Override
	public KeyVersion rollNewVersion(String name, byte[] material)
			throws IOException {
		if (logger.isDebugEnabled()) {
			logger.debug("==> RangerKeyStoreProvider.rollNewVersion()");
		}
		reloadKeys();
		Metadata meta = getMetadata(name);
		if (meta == null) {
			throw new IOException("Key " + name + " not found");
		}
		if (meta.getBitLength() != 8 * material.length) {
			throw new IOException("Wrong key length. Required "
					+ meta.getBitLength() + ", but got "
					+ (8 * material.length));
		}
		int nextVersion = meta.addVersion();
		String versionName = buildVersionName(name, nextVersion);
		return innerSetKeyVersion(name, versionName, material,
				meta.getCipher(), meta.getBitLength(), meta.getDescription(),
				meta.getVersions(), meta.getAttributes());
	}

	private static void getFromJceks(Configuration conf, String path,
			String alias, String key) {
		if (logger.isDebugEnabled()) {
			logger.debug("==> RangerKeyStoreProvider.getFromJceks()");
		}
		// update credential from keystore
		if (conf != null) {
			String pathValue = conf.get(path);
			String aliasValue = conf.get(alias);
			if (pathValue != null && aliasValue != null) {
				String xaDBPassword = CredentialReader.getDecryptedString(
						pathValue.trim(), aliasValue.trim());
				if (xaDBPassword != null && !xaDBPassword.trim().isEmpty()
						&& !xaDBPassword.trim().equalsIgnoreCase("none")) {
					conf.set(key, xaDBPassword);
				} else {
					logger.info("Credential keystore password not applied for KMS; clear text password shall be applicable");
				}
			}
		}
	}

	private void reloadKeys() throws IOException {
		if (logger.isDebugEnabled()) {
			logger.debug("==> RangerKeyStoreProvider.reloadKeys()");
		}
		try {
			cache.clear();
			loadKeys(masterKey);
		} catch (NoSuchAlgorithmException e) {
			throw new IOException("Can't load Keys");
		} catch (CertificateException e) {
			throw new IOException("Can't load Keys");
		}
	}

	/**
	 * The factory to create JksProviders, which is used by the ServiceLoader.
	 */
	public static class Factory extends KeyProviderFactory {
		@Override
		public KeyProvider createProvider(URI providerName, Configuration conf)
				throws IOException {
			try {
				if (SCHEME_NAME.equals(providerName.getScheme())) {
					return new RangerKeyStoreProvider(conf);
				}
			} catch (Throwable e) {
				logger.error(
						"==> RangerKeyStoreProvider.reloadKeys() error : ", e);
			}
			return null;
		}
	}

	/**
	 * An adapter between a KeyStore Key and our Metadata. This is used to store
	 * the metadata in a KeyStore even though isn't really a key.
	 */
	public static class KeyMetadata implements Key, Serializable {
		Metadata metadata;
		private final static long serialVersionUID = 8405872419967874451L;

		protected KeyMetadata(Metadata meta) {
			this.metadata = meta;
		}

		@Override
		public String getAlgorithm() {
			return metadata.getCipher();
		}

		@Override
		public String getFormat() {
			return KEY_METADATA;
		}

		@Override
		public byte[] getEncoded() {
			return new byte[0];
		}

		private void writeObject(ObjectOutputStream out) throws IOException {
			byte[] serialized = metadata.serialize();
			out.writeInt(serialized.length);
			out.write(serialized);
		}

		private void readObject(ObjectInputStream in) throws IOException,
				ClassNotFoundException {
			byte[] buf = new byte[in.readInt()];
			in.readFully(buf);
			metadata = new Metadata(buf);
		}

	}

}
