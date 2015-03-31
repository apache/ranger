package org.apache.hadoop.crypto.key;

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

import javax.crypto.spec.SecretKeySpec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.apache.hadoop.fs.Path;
import org.apache.ranger.kms.dao.DaoManager;

public class RangerKeyStoreProvider extends KeyProvider{
	
	public static final String SCHEME_NAME = "dbks";
	public static final String KMS_CONFIG_DIR = "kms.config.dir";
	public static final String DBKS_SITE_XML = "dbks-site.xml";
	public static final String ENCRYPTION_KEY = "ranger.db.encrypt.key.password";
	private static final String KEY_METADATA = "KeyMetadata";
	private final RangerKeyStore dbStore;
	private char[] masterKey;
	private boolean changed = false;
	private final Map<String, Metadata> cache = new HashMap<String, Metadata>();
	private DaoManager daoManager;

	public RangerKeyStoreProvider(Configuration conf) throws Throwable {
		super(conf);
		conf = getDBKSConf();
		RangerKMSDB rangerKMSDB = new RangerKMSDB(conf);
		daoManager = rangerKMSDB.getDaoManager();
		RangerMasterKey rangerMasterKey = new RangerMasterKey(daoManager);		
		dbStore = new RangerKeyStore(daoManager);
		String password = conf.get(ENCRYPTION_KEY);
		rangerMasterKey.generateMasterKey(password);		
		//code to retrieve rangerMasterKey password		
		masterKey = rangerMasterKey.getMasterKey(password).toCharArray();
		if(masterKey == null){
			// Master Key does not exists
	        throw new IOException("Ranger MasterKey does not exists");
		}
		try {
			loadKeys(masterKey);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			throw new IOException("Can't load Keys");
		}catch(CertificateException e){
			e.printStackTrace();
			throw new IOException("Can't load Keys");
		}
	}

	public static Configuration getDBKSConf() {
	    return getConfiguration(true, DBKS_SITE_XML);
	}
	
	static Configuration getConfiguration(boolean loadHadoopDefaults,
		      String ... resources) {
		    Configuration conf = new Configuration(loadHadoopDefaults);
		    String confDir = System.getProperty(KMS_CONFIG_DIR);
		    if (confDir != null) {
		      try {
		        Path confPath = new Path(confDir);
		        if (!confPath.isUriPathAbsolute()) {
		          throw new RuntimeException("System property '" + KMS_CONFIG_DIR +
		              "' must be an absolute path: " + confDir);
		        }
		        for (String resource : resources) {
		          conf.addResource(new URL("file://" + new Path(confDir, resource).toUri()));
		        }
		      } catch (MalformedURLException ex) {
		    	  ex.printStackTrace();
		        throw new RuntimeException(ex);
		      }
		    } else {
		      for (String resource : resources) {
		        conf.addResource(resource);
		      }
		    }
		    return conf;
		}
	
	private void loadKeys(char[] masterKey) throws NoSuchAlgorithmException, CertificateException, IOException {
		dbStore.engineLoad(null, masterKey);		
	}

	@Override
	public KeyVersion createKey(String name, byte[] material, Options options)
			throws IOException {
		  if (dbStore.engineContainsAlias(name) || cache.containsKey(name)) {
			  throw new IOException("Key " + name + " already exists");
		  }
	      Metadata meta = new Metadata(options.getCipher(), options.getBitLength(),
	          options.getDescription(), options.getAttributes(), new Date(), 1);
	      if (options.getBitLength() != 8 * material.length) {
	        throw new IOException("Wrong key length. Required " +
	            options.getBitLength() + ", but got " + (8 * material.length));
	      }
	      cache.put(name, meta);
	      String versionName = buildVersionName(name, 0);
	      return innerSetKeyVersion(name, versionName, material, meta.getCipher(), meta.getBitLength(), meta.getDescription(), meta.getVersions(), meta.getAttributes());
	}
	
	KeyVersion innerSetKeyVersion(String name, String versionName, byte[] material, String cipher, int bitLength, String description, int version, Map<String, String> attributes) throws IOException {
		try {
	          ObjectMapper om = new ObjectMapper();
	          String attribute = om.writeValueAsString(attributes);
			  dbStore.engineSetKeyEntry(versionName, new SecretKeySpec(material, cipher), masterKey, cipher, bitLength, description, version, attribute);
		} catch (KeyStoreException e) {
			e.printStackTrace();
			throw new IOException("Can't store key " + versionName,e);
		}
		changed = true;
		return new KeyVersion(name, versionName, material);
	}

	@Override
	public void deleteKey(String name) throws IOException {
	      Metadata meta = getMetadata(name);
	      if (meta == null) {
	        throw new IOException("Key " + name + " does not exist");
	      }
	      for(int v=0; v < meta.getVersions(); ++v) {
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
	    	  e.printStackTrace();
	        throw new IOException("Problem removing " + name + " from " + this, e);
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
	      for(Map.Entry<String, Metadata> entry: cache.entrySet()) {
	        try {
	          Metadata metadata = entry.getValue();
	          ObjectMapper om = new ObjectMapper();
	          String attributes = om.writeValueAsString(metadata.getAttributes());
	          dbStore.engineSetKeyEntry(entry.getKey(), new KeyMetadata(metadata), masterKey, metadata.getAlgorithm(), metadata.getBitLength(), metadata.getDescription(), metadata.getVersions(), attributes);
	        } catch (KeyStoreException e) {
	        	e.printStackTrace();
	          throw new IOException("Can't set metadata key " + entry.getKey(),e );
	        }
	      }
	      try {
	          dbStore.engineStore(null, masterKey);
	        } catch (NoSuchAlgorithmException e) {
	        	e.printStackTrace();
	          throw new IOException("No such algorithm storing key", e);
	        } catch (CertificateException e) {
	        	e.printStackTrace();
	          throw new IOException("Certificate exception storing key", e);
	        }
	      changed = false;
		 }catch (IOException ioe) {
			 ioe.printStackTrace();
	          throw ioe;
	     }
	}

	@Override
	public KeyVersion getKeyVersion(String versionName) throws IOException {
	      SecretKeySpec key = null;
	      try {
	        if (!dbStore.engineContainsAlias(versionName)) {
	          return null;
	        }
	        key = (SecretKeySpec) dbStore.engineGetKey(versionName, masterKey);
	      } catch (NoSuchAlgorithmException e) {
	    	  e.printStackTrace();
	        throw new IOException("Can't get algorithm for key " + key, e);
	      } catch (UnrecoverableKeyException e) {
	    	  e.printStackTrace();
	        throw new IOException("Can't recover key " + key, e);
	      }
		if (key == null) {
			return null;
		} else {
			return new KeyVersion(getBaseName(versionName), versionName, key.getEncoded());
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
		  if (cache.containsKey(name)) {
	        return cache.get(name);
	      }
	      try {
	        if (!dbStore.engineContainsAlias(name)) {
	          return null;
	        }
	        Metadata meta = ((KeyMetadata) dbStore.engineGetKey(name, masterKey)).metadata;
	        cache.put(name, meta);
	        return meta;
	      } catch (NoSuchAlgorithmException e) {
	    	  e.printStackTrace();
	        throw new IOException("Can't get algorithm for " + name, e);
	      } catch (UnrecoverableKeyException e) {
	    	  e.printStackTrace();
	        throw new IOException("Can't recover key for " + name, e);
	      }	      
	}

	@Override
	public KeyVersion rollNewVersion(String name, byte[] material)throws IOException {
		Metadata meta = getMetadata(name);
        if (meta == null) {
	        throw new IOException("Key " + name + " not found");
	    }
	    if (meta.getBitLength() != 8 * material.length) {
	        throw new IOException("Wrong key length. Required " + meta.getBitLength() + ", but got " + (8 * material.length));
	    }
	    int nextVersion = meta.addVersion();
	    String versionName = buildVersionName(name, nextVersion);
	    return innerSetKeyVersion(name, versionName, material, meta.getCipher(), meta.getBitLength(), meta.getDescription(), meta.getVersions(), meta.getAttributes());
	}
	
	/**
	 * The factory to create JksProviders, which is used by the ServiceLoader.
	*/
	public static class Factory extends KeyProviderFactory {
	    @Override
	    public KeyProvider createProvider(URI providerName,
	                                      Configuration conf) throws IOException {
	        try {
	        	if (SCHEME_NAME.equals(providerName.getScheme())) {
	        		return new RangerKeyStoreProvider(conf);
	            }				
			} catch (Throwable e) {
				e.printStackTrace();
			}
	        return null;
	    }
	}
	
	  /**
	   * An adapter between a KeyStore Key and our Metadata. This is used to store
	   * the metadata in a KeyStore even though isn't really a key.
	   */
	  public static class KeyMetadata implements Key, Serializable {
	    private Metadata metadata;
	    private final static long serialVersionUID = 8405872419967874451L;

	    private KeyMetadata(Metadata meta) {
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

	    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
	      byte[] buf = new byte[in.readInt()];
	      in.readFully(buf);
	      metadata = new Metadata(buf);
	    }

	}
}