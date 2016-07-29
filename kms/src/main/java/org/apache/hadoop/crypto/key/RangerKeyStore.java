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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.DigestInputStream;
import java.security.DigestOutputStream;
import java.security.Key;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.KeyStoreSpi;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;

import javax.crypto.SealedObject;
import javax.xml.bind.DatatypeConverter;

import org.apache.hadoop.crypto.key.KeyProvider.Metadata;
import org.apache.log4j.Logger;
import org.apache.ranger.entity.XXRangerKeyStore;
import org.apache.ranger.kms.dao.DaoManager;
import org.apache.ranger.kms.dao.RangerKMSDao;

/**
 * This class provides the Database store implementation.
 */

public class RangerKeyStore extends KeyStoreSpi {
	
	static final Logger logger = Logger.getLogger(RangerKeyStore.class);
		
	private DaoManager daoManager;
	
    // keys
    private static class KeyEntry {
        Date date=new Date(); // the creation date of this entry
    };

    // Secret key
    private static final class SecretKeyEntry {
        Date date=new Date(); // the creation date of this entry
        SealedObject sealedKey;
        String cipher_field;
        int bit_length;
        String description;
        String attributes;
        int version;
    }

    private Hashtable<String, Object> keyEntries = new Hashtable<String, Object>();
    private Hashtable<String, Object> deltaEntries = new Hashtable<String, Object>();
    
    RangerKeyStore() {
    }

    RangerKeyStore(DaoManager daoManager) {
    	this.daoManager = daoManager;
	}

    String convertAlias(String alias){
    	return alias.toLowerCase();
    }

    @Override
    public Key engineGetKey(String alias, char[] password)throws NoSuchAlgorithmException, UnrecoverableKeyException
    {
    	Key key = null;

        Object entry = keyEntries.get(alias.toLowerCase());

        if (!(entry instanceof SecretKeyEntry)) {
            return null;
        }
        
        Class<?> c = null;
    	Object o = null;
		try {
			c = Class.forName("com.sun.crypto.provider.KeyProtector");
			Constructor<?> constructor = c.getDeclaredConstructor(char[].class);
	        constructor.setAccessible(true);
	        o = constructor.newInstance(password);	 
	        Method m = c.getDeclaredMethod("unseal", SealedObject.class);
            m.setAccessible(true);
			key = (Key) m.invoke(o, ((SecretKeyEntry)entry).sealedKey);
		} catch (ClassNotFoundException | NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
			logger.error(e.getMessage());
		}
        return key;        
    }

    @Override
    public Date engineGetCreationDate(String alias) {
        Object entry = keyEntries.get(convertAlias(alias));
        Date date=null;
        if (entry != null) {
			KeyEntry keyEntry=(KeyEntry)entry;
			if(keyEntry.date!=null){
				date=new Date(keyEntry.date.getTime());
			}
		}
		return date;
	}


    public void addKeyEntry(String alias, Key key, char[] password, String cipher, int bitLength, String description, int version, String attributes)
        throws KeyStoreException
    {
    	SecretKeyEntry entry = new SecretKeyEntry();
        synchronized(deltaEntries) {
            try {            	
            	Class<?> c = null;
            	Object o = null;
        		try {
        			c = Class.forName("com.sun.crypto.provider.KeyProtector");
        			Constructor<?> constructor = c.getDeclaredConstructor(char[].class);
        	        constructor.setAccessible(true);
        	        o = constructor.newInstance(password);        	        
        		} catch (ClassNotFoundException | NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
        			logger.error(e.getMessage());
        			throw new KeyStoreException(e.getMessage());
        		}
                entry.date = new Date();
                // seal and store the key
                Method m = c.getDeclaredMethod("seal", Key.class);
                m.setAccessible(true);
                entry.sealedKey = (SealedObject) m.invoke(o, key);
                
                entry.cipher_field = cipher;
                entry.bit_length = bitLength;
                entry.description = description;
                entry.version = version;
                entry.attributes = attributes;
                deltaEntries.put(alias.toLowerCase(), entry);                       
            } catch (Exception e) {
            	logger.error(e.getMessage());
            	throw new KeyStoreException(e.getMessage());
            }      
        }
        synchronized(keyEntries) {
        	try {
        		keyEntries.put(alias.toLowerCase(), entry);
        	}catch (Exception e) {
            	logger.error(e.getMessage());
            	throw new KeyStoreException(e.getMessage());
            }  
        }
    }

    @Override
    public void engineDeleteEntry(String alias)
        throws KeyStoreException
    {
        synchronized(keyEntries) {
        		dbOperationDelete(convertAlias(alias));
        		keyEntries.remove(convertAlias(alias));        	
        }
        synchronized(deltaEntries) {
        	deltaEntries.remove(convertAlias(alias));
        }
    }

    
    private void dbOperationDelete(String alias) {
    	try{
			  if(daoManager != null){
				  RangerKMSDao rangerKMSDao = new RangerKMSDao(daoManager);			  
				  rangerKMSDao.deleteByAlias(alias);
			  }			  
		}catch(Exception e){
			logger.error(e.getMessage());
			e.printStackTrace();
		}
	}


    @Override
    public Enumeration<String> engineAliases() {
        return keyEntries.keys();
    }

    @Override
    public boolean engineContainsAlias(String alias) {
        return keyEntries.containsKey(convertAlias(alias));
    }

    @Override
    public int engineSize() {
        return keyEntries.size();
    }

    @Override
    public void engineStore(OutputStream stream, char[] password)
        throws IOException, NoSuchAlgorithmException, CertificateException
    {
        synchronized(deltaEntries) {
            // password is mandatory when storing
            if (password == null) {
                throw new IllegalArgumentException("Ranger Master Key can't be null");
            }

            MessageDigest md = getKeyedMessageDigest(password);
            
           	byte digest[] = md.digest();    
           	for (Enumeration<String> e = deltaEntries.keys(); e.hasMoreElements();) {           		
            	ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(new DigestOutputStream(baos, md));
                
                ObjectOutputStream oos = null;
            	try{
            	
            		String alias = e.nextElement();
            		Object entry = deltaEntries.get(alias);

                    oos = new ObjectOutputStream(dos);
                    oos.writeObject(((SecretKeyEntry)entry).sealedKey);
                    
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
           	clearDeltaEntires();
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
  			logger.error(e.getMessage());
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


	@Override
	public void engineLoad(InputStream stream, char[] password)
        throws IOException	, NoSuchAlgorithmException, CertificateException
    {
        synchronized(keyEntries) {
        	List<XXRangerKeyStore> rangerKeyDetails = dbOperationLoad();
        		
            DataInputStream dis;
            MessageDigest md = null;
           
			if(rangerKeyDetails == null || rangerKeyDetails.size() < 1){
        		return;
        	}
			
			keyEntries.clear();     
			if(password!=null){
				md = getKeyedMessageDigest(password);
			}

			byte computed[]={};
            if(md!=null){
				computed = md.digest();
			}
            for(XXRangerKeyStore rangerKey : rangerKeyDetails){
            	String encoded = rangerKey.getEncoded();
            	byte[] data = DatatypeConverter.parseBase64Binary(encoded);
            	
            	if(data  != null && data.length > 0){
            		stream = new ByteArrayInputStream(data);
            	}else{
            		logger.error("No Key found for alias "+rangerKey.getAlias());
            	}
            	
	             if (computed != null) {	                
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
					keyEntries.put(alias, entry);		            
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
				  return rangerKMSDao.getAllKeys();
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
    
    private final String SECRET_KEY_HASH_WORD = "Apache Ranger" ;
    
    private MessageDigest getKeyedMessageDigest(char[] aKeyPassword)
        throws NoSuchAlgorithmException, UnsupportedEncodingException
    {
        int i, j;

        MessageDigest md = MessageDigest.getInstance("SHA");
        byte[] keyPasswordBytes = new byte[aKeyPassword.length * 2];
        for (i=0, j=0; i<aKeyPassword.length; i++) {
            keyPasswordBytes[j++] = (byte)(aKeyPassword[i] >> 8);
            keyPasswordBytes[j++] = (byte)aKeyPassword[i];
        }
        md.update(keyPasswordBytes);
        for (i=0; i<keyPasswordBytes.length; i++)
            keyPasswordBytes[i] = 0;
        md.update(SECRET_KEY_HASH_WORD.getBytes("UTF8"));
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

	//
	// The method is created to support JKS migration (from hadoop-common KMS keystore to RangerKMS keystore)
	//
	
	private static final String METADATA_FIELDNAME = "metadata" ;
	private static final int NUMBER_OF_BITS_PER_BYTE = 8 ;
	
	public void engineLoadKeyStoreFile(InputStream stream, char[] storePass, char[] keyPass, char[] masterKey, String fileFormat)
	        throws IOException, NoSuchAlgorithmException, CertificateException
	{
			synchronized(deltaEntries) {
				KeyStore ks;
				
				try {
					ks = KeyStore.getInstance(fileFormat);
					ks.load(stream, storePass);
					deltaEntries.clear();     
					for (Enumeration<String> name = ks.aliases(); name.hasMoreElements();){
						  	  SecretKeyEntry entry = new SecretKeyEntry();
							  String alias = (String) name.nextElement();
							  Key k = ks.getKey(alias, keyPass);		
							  
							  if (k instanceof JavaKeyStoreProvider.KeyMetadata) {
								  JavaKeyStoreProvider.KeyMetadata keyMetadata = (JavaKeyStoreProvider.KeyMetadata)k ; 
								  Field f = JavaKeyStoreProvider.KeyMetadata.class.getDeclaredField(METADATA_FIELDNAME) ;
								  f.setAccessible(true);
								  Metadata metadata = (Metadata)f.get(keyMetadata) ;
								  entry.bit_length = metadata.getBitLength() ;
								  entry.cipher_field = metadata.getAlgorithm() ;
								  Constructor<RangerKeyStoreProvider.KeyMetadata> constructor = RangerKeyStoreProvider.KeyMetadata.class.getDeclaredConstructor(Metadata.class);
							      constructor.setAccessible(true);
							      RangerKeyStoreProvider.KeyMetadata  nk = constructor.newInstance(metadata);
							      k = nk ;
							  }
							  else {
			                      entry.bit_length = (k.getEncoded().length * NUMBER_OF_BITS_PER_BYTE) ;
			                      entry.cipher_field = k.getAlgorithm();
							  }
		                      String keyName = alias.split("@")[0] ;
		                      entry.attributes = "{\"key.acl.name\":\"" +  keyName + "\"}" ;
		                      Class<?> c = null;
		                  	  Object o = null;
		                  	  try {
		              			c = Class.forName("com.sun.crypto.provider.KeyProtector");
		              			Constructor<?> constructor = c.getDeclaredConstructor(char[].class);
		              	        constructor.setAccessible(true);
		              	        o = constructor.newInstance(masterKey);     
		              	        // seal and store the key
			                    Method m = c.getDeclaredMethod("seal", Key.class);
			                    m.setAccessible(true);
			                    entry.sealedKey = (SealedObject) m.invoke(o, k);
		                  	  } catch (ClassNotFoundException | NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
		                  		  logger.error(e.getMessage());
		                  		  throw new IOException(e.getMessage());
		                  	  }
		                  	  
 	                          entry.date = ks.getCreationDate(alias);
		                      entry.version = (alias.split("@").length == 2)?(Integer.parseInt(alias.split("@")[1])):0;
		    				  entry.description = k.getFormat()+" - "+ks.getType();
		    	              deltaEntries.put(alias, entry);		
		                    }
				} catch (Throwable t) {
					logger.error("Unable to load keystore file ", t);
					throw new IOException(t) ;
				}
			}
	}
	
	public void engineLoadToKeyStoreFile(OutputStream stream, char[] storePass, char[] keyPass, char[] masterKey, String fileFormat)
	        throws IOException, NoSuchAlgorithmException, CertificateException
	{
			synchronized(keyEntries) {
				KeyStore ks;
				try {
					ks = KeyStore.getInstance(fileFormat);
					if(ks!=null){
						ks.load(null, storePass);
						String alias = null;
						engineLoad(null, masterKey);
						Enumeration<String> e = engineAliases();
						Key key;
						while (e.hasMoreElements()) {
							alias = e.nextElement();
							key = engineGetKey(alias, masterKey);
							ks.setKeyEntry(alias, key, keyPass, null);
						}
						ks.store(stream, storePass);
					}
				} catch (Throwable t) {
					logger.error("Unable to load keystore file ", t);
					throw new IOException(t) ;
				}
			}
	}
	
	public void clearDeltaEntires(){
		deltaEntries.clear();
	}
	
}