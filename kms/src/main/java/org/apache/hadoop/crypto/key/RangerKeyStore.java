package org.apache.hadoop.crypto.key;

import java.io.*;
import java.security.*;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.util.*;

import javax.crypto.SealedObject;
import javax.xml.bind.DatatypeConverter;

import org.apache.log4j.Logger;
import org.apache.ranger.entity.XXRangerKeyStore;
import org.apache.ranger.kms.dao.DaoManager;
import org.apache.ranger.kms.dao.RangerKMSDao;

/**
 * This class provides the Database store implementation.
 *
 *
 * @see KeyProtector
 *
 */

public class RangerKeyStore extends KeyStoreSpi {
	
	static final Logger logger = Logger.getLogger(RangerKeyStore.class);
		
	private DaoManager daoManager;
	
    // keys
    private static class KeyEntry {
        Date date; // the creation date of this entry
    };

    // Secret key
    private static final class SecretKeyEntry {
        Date date; // the creation date of this entry
        SealedObject sealedKey;
        String cipher_field;
        int bit_length;
        String description;
        String attributes;
        int version;
    }

    /**
     * keys are stored in a hashtable.
     * Hash entries are keyed by alias names.
     */
    private final Hashtable<String, Object> entries;
    
    RangerKeyStore() {
        entries = new Hashtable<String, Object>();
    }

    RangerKeyStore(DaoManager daoManager) {
    	entries = new Hashtable<String, Object>();
    	this.daoManager = daoManager;
	}

	// convert an alias to internal form, overridden in subclasses:
    String convertAlias(String alias){
    	return alias.toLowerCase();
    }

    /**
     * Returns the key associated with the given alias, using the given
     * password to recover it.
     *
     * @param alias the alias name
     * @param password the password for recovering the key
     *
     * @return the requested key, or null if the given alias does not exist
     * or does not identify a <i>key entry</i>.
     *
     * @exception NoSuchAlgorithmException if the algorithm for recovering the
     * key cannot be found
     * @exception UnrecoverableKeyException if the key cannot be recovered
     * (e.g., the given password is wrong).
     */
    public Key engineGetKey(String alias, char[] password)
        throws NoSuchAlgorithmException, UnrecoverableKeyException
    {
    	Key key = null;

        Object entry = entries.get(alias.toLowerCase());

        if (!(entry instanceof SecretKeyEntry)) {
            return null;
        }

        KeyProtector keyProtector = new KeyProtector(password);
        key = keyProtector.unseal(((SecretKeyEntry)entry).sealedKey);
        return key;        
    }

    /**
     * Returns the creation date of the entry identified by the given alias.
     *
     * @param alias the alias name
     *
     * @return the creation date of this entry, or null if the given alias does
     * not exist
     */
    public Date engineGetCreationDate(String alias) {
        Object entry = entries.get(convertAlias(alias));
        if (entry != null) {
               return new Date(((KeyEntry)entry).date.getTime());
        } else {
            return null;
        }
    }

    /**
     * Assigns the given key to the given alias, protecting
     * it with the given password as defined in PKCS8.
     *
     * <p>The given java.security.PrivateKey <code>key</code> must
     * be accompanied by a certificate chain certifying the
     * corresponding public key.
     *
     * <p>If the given alias already exists, the keystore information
     * associated with it is overridden by the given key and certificate
     * chain.
     *
     * @param alias the alias name
     * @param key the key to be associated with the alias
     * @param password the password to protect the key
     * @param cipher the cipher used for the key
     * @param bitLength bit length for the key
     * @param description Description for the key
     * @param version Key version
     * @param attributes key attributes 
     *
     * @exception KeyStoreException if the given key is not a private key,
     * cannot be protected, or this operation fails for some other reason
     */
    public void engineSetKeyEntry(String alias, Key key, char[] password, String cipher, int bitLength, String description, int version, String attributes)
        throws KeyStoreException
    {
        synchronized(entries) {
            try {
                KeyProtector keyProtector = new KeyProtector(password);

                SecretKeyEntry entry = new SecretKeyEntry();
                entry.date = new Date();
                // seal and store the key
                entry.sealedKey = keyProtector.seal(key);
                entry.cipher_field = cipher;
                entry.bit_length = bitLength;
                entry.description = description;
                entry.version = version;
                entry.attributes = attributes;
                entries.put(alias.toLowerCase(), entry);                
            } catch (Exception e) {
                throw new KeyStoreException(e.getMessage());
            }      
        }
    }

    /**
     * Deletes the entry identified by the given alias from this database.
     *
     * @param alias the alias name
     *
     * @exception KeyStoreException if the entry cannot be removed.
     */
    public void engineDeleteEntry(String alias)
        throws KeyStoreException
    {
        synchronized(entries) {
        		dbOperationDelete(convertAlias(alias));
        		entries.remove(convertAlias(alias));	
        }
    }

    private void dbOperationDelete(String alias) {
    	try{
			  if(daoManager != null){
				  RangerKMSDao rangerKMSDao = new RangerKMSDao(daoManager);			  
				  rangerKMSDao.deleteByAlias(alias);
			  }			  
		}catch(Exception e){
			e.printStackTrace();
		}
	}

	/**
     * Lists all the alias names of this database.
     *
     * @return enumeration of the alias names
     */
    public Enumeration<String> engineAliases() {
        return entries.keys();
    }

    /**
     * Checks if the given alias exists in this database.
     *
     * @param alias the alias name
     *
     * @return true if the alias exists, false otherwise
     */
    public boolean engineContainsAlias(String alias) {
        return entries.containsKey(convertAlias(alias));
    }

    /**
     * Retrieves the number of entries in this database.
     *
     * @return the number of entries in this database
     */
    public int engineSize() {
        return entries.size();
    }

    /**
     * Stores this keystore to the provided ranger database, and protects its
     * integrity with the given password.
     *
     * @param stream null.
     * @param password the password to generate the keystore integrity check
     *
     * @exception IOException if there was an I/O problem with data
     * @exception NoSuchAlgorithmException if the appropriate data integrity
     * algorithm could not be found
     * @exception CertificateException if any of the certificates included in
     * the keystore data could not be stored
     */
    public void engineStore(OutputStream stream, char[] password)
        throws IOException, NoSuchAlgorithmException, CertificateException
    {
        synchronized(entries) {
            // password is mandatory when storing
            if (password == null) {
                throw new IllegalArgumentException("Ranger Master Key can't be null");
            }

            MessageDigest md = getPreKeyedHash(password);            
            
           	byte digest[] = md.digest();    
           	for (Enumeration<String> e = entries.keys(); e.hasMoreElements();) {
            	ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(new DigestOutputStream(baos, md));
                
                ObjectOutputStream oos = null;
            	try{
            	
            		String alias = e.nextElement();
            		Object entry = entries.get(alias);

                    // write the sealed key
                    oos = new ObjectOutputStream(dos);
                    oos.writeObject(((SecretKeyEntry)entry).sealedKey);
                    /*
                     * Write the keyed hash which is used to detect tampering with
                     * the keystore (such as deleting or modifying key or
                     * certificate entries).
                     */
                    dos.write(digest);
                    dos.flush();
                    Long creationDate = ((SecretKeyEntry)entry).date.getTime();
                    SecretKeyEntry secretKey = (SecretKeyEntry)entry;
                    XXRangerKeyStore xxRangerKeyStore = mapObjectToEntity(alias,creationDate,baos.toByteArray(), secretKey.cipher_field, secretKey.bit_length, secretKey.description, secretKey.version, secretKey.attributes);
                    dbOperationStore(xxRangerKeyStore);
            	}finally {
                    if (oos != null) {
                        oos.close();
                    } else {
                        dos.close();
                    }
                }                
            }
        }
    }

    private XXRangerKeyStore mapObjectToEntity(String alias, Long creationDate,
		byte[] byteArray, String cipher_field, int bit_length,
		String description, int version, String attributes) {
    	XXRangerKeyStore xxRangerKeyStore = new XXRangerKeyStore();
    	xxRangerKeyStore.setAlias(alias);
    	xxRangerKeyStore.setCreatedDate(creationDate);
    	xxRangerKeyStore.setEncoded(DatatypeConverter.printBase64Binary(byteArray));
    	xxRangerKeyStore.setCipher(cipher_field);
    	xxRangerKeyStore.setBitLength(bit_length);
    	xxRangerKeyStore.setDescription(description);
    	xxRangerKeyStore.setVersion(version);
    	xxRangerKeyStore.setAttributes(attributes);
		return xxRangerKeyStore;
	}

	private void dbOperationStore(XXRangerKeyStore rangerKeyStore) {
		try{
			  if(daoManager != null){
				  RangerKMSDao rangerKMSDao = new RangerKMSDao(daoManager);
				  XXRangerKeyStore xxRangerKeyStore = rangerKMSDao.findByAlias(rangerKeyStore.getAlias());
				  boolean keyStoreExists = true;
				  if (xxRangerKeyStore == null) {
					  xxRangerKeyStore = new XXRangerKeyStore();
					  keyStoreExists = false;
				  }

				  xxRangerKeyStore = mapToEntityBean(rangerKeyStore, xxRangerKeyStore, 0);		
				  if (keyStoreExists) {
					  xxRangerKeyStore = rangerKMSDao.update(xxRangerKeyStore);
				  } else {
					  xxRangerKeyStore = rangerKMSDao.create(xxRangerKeyStore);
				  }
			  }
  		}catch(Exception e){
  			e.printStackTrace();
  		}
	}

	private XXRangerKeyStore mapToEntityBean(XXRangerKeyStore rangerKMSKeyStore, XXRangerKeyStore xxRangerKeyStore,int i) {
		xxRangerKeyStore.setAlias(rangerKMSKeyStore.getAlias());
		xxRangerKeyStore.setCreatedDate(rangerKMSKeyStore.getCreatedDate());
		xxRangerKeyStore.setEncoded(rangerKMSKeyStore.getEncoded());
		xxRangerKeyStore.setCipher(rangerKMSKeyStore.getCipher());
		xxRangerKeyStore.setBitLength(rangerKMSKeyStore.getBitLength());
		xxRangerKeyStore.setDescription(rangerKMSKeyStore.getDescription());
		xxRangerKeyStore.setVersion(rangerKMSKeyStore.getVersion());
		xxRangerKeyStore.setAttributes(rangerKMSKeyStore.getAttributes());
		return xxRangerKeyStore;
	}

	/**
     * Loads the keystore from the given ranger database.
     *
     * <p>If a password is given, it is used to check the integrity of the
     * keystore data. Otherwise, the integrity of the keystore is not checked.
     *
     * @param stream the input stream from which the keystore is loaded
     * @param password the (optional) password used to check the integrity of
     * the keystore.
     *
     * @exception IOException if there is an I/O or format problem with the
     * keystore data
     * @exception NoSuchAlgorithmException if the algorithm used to check
     * the integrity of the keystore cannot be found
     * @exception CertificateException if any of the certificates in the
     * keystore could not be loaded
     */
    public void engineLoad(InputStream stream, char[] password)
        throws IOException	, NoSuchAlgorithmException, CertificateException
    {
        synchronized(entries) {
        	List<XXRangerKeyStore> rangerKeyDetails = dbOperationLoad();
            DataInputStream dis;
            MessageDigest md = null;
           
			if(rangerKeyDetails == null || rangerKeyDetails.size() < 1){
        		return;
        	}
			
			entries.clear();     
			md = getPreKeyedHash(password);

			byte computed[];
            computed = md.digest();
            for(XXRangerKeyStore rangerKey : rangerKeyDetails){
            	String encoded = rangerKey.getEncoded();
            	byte[] data = DatatypeConverter.parseBase64Binary(encoded);
            	
            	if(data  != null && data.length > 0){
            		stream = new ByteArrayInputStream(data);
            	}else{
            		logger.error("No Key found for alias "+rangerKey.getAlias());
            	}
            	
            	/*
	             * If a password has been provided, we check the keyed digest
	             * at the end. If this check fails, the store has been tampered
	             * with
	             */
	             if (password != null) {	                
	                int counter = 0; 
	                for (int i = computed.length-1; i >= 0; i--) {
	                    if (computed[i] != data[data.length-(1+counter)]) {
	                        Throwable t = new UnrecoverableKeyException
	                            ("Password verification failed");
	                        throw (IOException)new IOException
	                            ("Keystore was tampered with, or "
	                            + "password was incorrect").initCause(t);
	                    }else{
	                    	counter++;
	                    }
	                }
	             }
            	
				if (password != null) {
					dis = new DataInputStream(new DigestInputStream(stream, md));
				} else {
					dis = new DataInputStream(stream);
				}
				
				ObjectInputStream ois = null;
				try{
					String alias;

					SecretKeyEntry entry = new SecretKeyEntry();

					//read the alias
					alias = rangerKey.getAlias();

					//read the (entry creation) date
					entry.date = new Date(rangerKey.getCreatedDate());
					entry.cipher_field = rangerKey.getCipher();
					entry.bit_length = rangerKey.getBitLength();
					entry.description = rangerKey.getDescription();
					entry.version = rangerKey.getVersion();
					entry.attributes = rangerKey.getAttributes();

					//read the sealed key
					try {
						ois = new ObjectInputStream(dis);
						entry.sealedKey = (SealedObject)ois.readObject();
					} catch (ClassNotFoundException cnfe) {
						throw new IOException(cnfe.getMessage());
					}

					//Add the entry to the list
					entries.put(alias, entry);		            
				 }finally {
	                if (ois != null) {
	                    ois.close();
	                } else {
	                    dis.close();
	                }
	            }
            }
        }
    }

    private List<XXRangerKeyStore> dbOperationLoad() throws IOException {
    		try{
			  if(daoManager != null){
				  RangerKMSDao rangerKMSDao = new RangerKMSDao(daoManager);
				  return rangerKMSDao.getAll();
			  }			  
    		}catch(Exception e){
    			e.printStackTrace();
    		}
			return null;
	}

	/**
     * To guard against tampering with the keystore, we append a keyed
     * hash with a bit of whitener.
     */
    private MessageDigest getPreKeyedHash(char[] password)
        throws NoSuchAlgorithmException, UnsupportedEncodingException
    {
        int i, j;

        MessageDigest md = MessageDigest.getInstance("SHA");
        byte[] passwdBytes = new byte[password.length * 2];
        for (i=0, j=0; i<password.length; i++) {
            passwdBytes[j++] = (byte)(password[i] >> 8);
            passwdBytes[j++] = (byte)password[i];
        }
        md.update(passwdBytes);
        for (i=0; i<passwdBytes.length; i++)
            passwdBytes[i] = 0;
        md.update("Mighty Aphrodite".getBytes("UTF8"));
        return md;
    }

	@Override
	public void engineSetKeyEntry(String arg0, byte[] arg1, Certificate[] arg2)
			throws KeyStoreException {	
		
	}

	@Override
	public Certificate engineGetCertificate(String alias) {
		return null;
	}

	@Override
	public String engineGetCertificateAlias(Certificate cert) {
		return null;
	}

	@Override
	public Certificate[] engineGetCertificateChain(String alias) {
		return null;
	}

	@Override
	public boolean engineIsCertificateEntry(String alias) {
		return false;
	}

	@Override
	public boolean engineIsKeyEntry(String alias) {
		return false;
	}

	@Override
	public void engineSetCertificateEntry(String alias, Certificate cert)
			throws KeyStoreException {
	}

	@Override
	public void engineSetKeyEntry(String alias, Key key, char[] password,
			Certificate[] chain) throws KeyStoreException {
	}
	
	public Map<String, String> getPropertiesWithPrefix(Properties props, String prefix) {
		Map<String, String> prefixedProperties = new HashMap<String, String>();

		if(props != null && prefix != null) {
			for(String key : props.stringPropertyNames()) {
				if(key == null) {
					continue;
				}

				String val = props.getProperty(key);

				if(key.startsWith(prefix)) {
					key = key.substring(prefix.length());

					if(key == null) {
						continue;
					}

					prefixedProperties.put(key, val);
				}
			}
		}

		return prefixedProperties;
	}	
}