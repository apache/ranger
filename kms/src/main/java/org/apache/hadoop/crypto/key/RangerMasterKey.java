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

import java.security.Key;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.PBEParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.apache.log4j.Logger;
import org.apache.ranger.kms.dao.DaoManager;
import org.apache.ranger.kms.dao.RangerMasterKeyDao;
import org.apache.ranger.entity.XXRangerMasterKey;

import com.sun.org.apache.xml.internal.security.exceptions.Base64DecodingException;
import com.sun.org.apache.xml.internal.security.utils.Base64;

public class RangerMasterKey implements RangerKMSMKI{
	
	static final Logger logger = Logger.getLogger(RangerMasterKey.class);
	
	private static final String MK_CIPHER = "AES";
	private static final int MK_KeySize = 256;
	private static final int SALT_SIZE = 8;
	private static final String PBE_ALGO = "PBEWithMD5AndTripleDES";
	private static final String MD_ALGO = "MD5";
	
	private DaoManager daoManager;
	
	public RangerMasterKey() {		
	}
	
	public RangerMasterKey(DaoManager daoManager) {
		this.daoManager = daoManager;
	}
	
	/**
	 * To get Master Key
	 * @param password password to be used for decryption 
	 * @return Decrypted Master Key
	 * @throws Throwable 
	 */
	@Override
	public String getMasterKey(String password) throws Throwable{		
		logger.info("Getting Master Key");
		byte masterKeyByte[] = getEncryptedMK();
		if(masterKeyByte != null && masterKeyByte.length > 0){
			String masterKey = decryptMasterKey(masterKeyByte, password);		
			return masterKey;
		}else{
			throw new Exception("No Master Key Found");
		}			
	}
	
	public SecretKey getMasterSecretKey(String password) throws Throwable{		
		logger.info("Getting Master Key");
		byte masterKeyByte[] = getEncryptedMK();
		if(masterKeyByte != null && masterKeyByte.length > 0){
			return decryptMasterKeySK(masterKeyByte, password);		
		}else{
			throw new Exception("No Master Key Found");
		}			
	}
	
	/**
	 * Generate the master key encrypt's it and save it in database
	 * @param password password to be used for encryption
	 * @return true if successfully created the master key
	 * 		   false if master key generation was unsuccessful or already master key exists
	 * @throws Throwable 
	 */
	@Override
	public boolean generateMasterKey(String password) throws Throwable{
		logger.info("Generating Master Key");
		String encryptedMasterKey = encryptMasterKey(password);		
		String savedKey = saveEncryptedMK(encryptedMasterKey, daoManager);
		if(savedKey != null && !savedKey.trim().equals("")){
			logger.debug("Master Key Created with id = "+savedKey);
			return true;
		}
		return false;
	}
	
	public boolean generateMKFromHSMMK(String password, byte[] key) throws Throwable{
		logger.info("Generating Master Key");
		String encryptedMasterKey = encryptMasterKey(password, key);		
		String savedKey = saveEncryptedMK(encryptedMasterKey, daoManager);
		if(savedKey != null && !savedKey.trim().equals("")){
			logger.debug("Master Key Created with id = "+savedKey);
			return true;
		}
		return false;
	}

	private String decryptMasterKey(byte masterKey[], String password) throws Throwable {
		logger.debug("Decrypting Master Key");
		PBEKeySpec pbeKeyspec = getPBEParameterSpec(password) ;
		byte[] masterKeyFromDBDecrypted = decryptKey(masterKey, pbeKeyspec) ;
		SecretKey masterKeyFromDB = getMasterKeyFromBytes(masterKeyFromDBDecrypted) ;
		return Base64.encode(masterKeyFromDB.getEncoded());
	}
	
	private SecretKey decryptMasterKeySK(byte masterKey[], String password) throws Throwable {
		logger.debug("Decrypting Master Key");
		PBEKeySpec pbeKeyspec = getPBEParameterSpec(password) ;
		byte[] masterKeyFromDBDecrypted = decryptKey(masterKey, pbeKeyspec) ;
		return getMasterKeyFromBytes(masterKeyFromDBDecrypted) ;		
	}

	private byte[] getEncryptedMK() throws Base64DecodingException {
		logger.debug("Retrieving Encrypted Master Key from database");
		try{
			  if(daoManager != null){
				  RangerMasterKeyDao rangerKMSDao = new RangerMasterKeyDao(daoManager);
				  List<XXRangerMasterKey> lstRangerMasterKey = rangerKMSDao.getAll();
				  if(lstRangerMasterKey.size() < 1){
					  throw new Exception("No Master Key exists");
				  }else if(lstRangerMasterKey.size() > 1){
					  throw new Exception("More than one Master Key exists");
				  }else {
					  XXRangerMasterKey rangerMasterKey = rangerKMSDao.getById(lstRangerMasterKey.get(0).getId());
					  String masterKeyStr = rangerMasterKey.getMasterKey();
					  byte[] masterKeyFromDBEncrypted = Base64.decode(masterKeyStr) ;
					  return masterKeyFromDBEncrypted;
				  }
			  }			  
		  }catch(Exception e){
			  e.printStackTrace();
		  }
		  return null;
	}

	private String saveEncryptedMK(String encryptedMasterKey, DaoManager daoManager) {
		  logger.debug("Saving Encrypted Master Key to database");
		  XXRangerMasterKey xxRangerMasterKey = new XXRangerMasterKey();
		  xxRangerMasterKey.setCipher(MK_CIPHER);
		  xxRangerMasterKey.setBitLength(MK_KeySize);
		  xxRangerMasterKey.setMasterKey(encryptedMasterKey);
		  try{
			  if(daoManager != null){
				  RangerMasterKeyDao rangerKMSDao = new RangerMasterKeyDao(daoManager);
				  Long l = rangerKMSDao.getAllCount();
				  if(l < 1){
					  XXRangerMasterKey rangerMasterKey = rangerKMSDao.create(xxRangerMasterKey);
					  return rangerMasterKey.getId().toString();
				  }
			  }			  
		  }catch(Exception e){
			  e.printStackTrace();
		  }
		  return null;
	}

	private String encryptMasterKey(String password) throws Throwable {
			logger.debug("Encrypting Master Key");
			Key secretKey = generateMasterKey();
			PBEKeySpec pbeKeySpec = getPBEParameterSpec(password);
			byte[] masterKeyToDB = encryptKey(secretKey.getEncoded(), pbeKeySpec);
			String masterKey = Base64.encode(masterKeyToDB) ;
			return masterKey;
	}
	
	private String encryptMasterKey(String password, byte[] secretKey) throws Throwable {
		logger.debug("Encrypting Master Key");
		PBEKeySpec pbeKeySpec = getPBEParameterSpec(password);
		byte[] masterKeyToDB = encryptKey(secretKey, pbeKeySpec);
		String masterKey = Base64.encode(masterKeyToDB) ;
		return masterKey;
	}
	
	private Key generateMasterKey() throws NoSuchAlgorithmException{
		KeyGenerator kg = KeyGenerator.getInstance(MK_CIPHER);
		kg.init(MK_KeySize);
		return kg.generateKey();
	}
	
	private PBEKeySpec getPBEParameterSpec(String password) throws Throwable {
		MessageDigest md = MessageDigest.getInstance(MD_ALGO) ;
		byte[] saltGen = md.digest(password.getBytes()) ;		 
		byte[] salt = new byte[SALT_SIZE] ;		 
		System.arraycopy(saltGen, 0, salt, 0, SALT_SIZE);		 
		int iteration = password.toCharArray().length + 1 ;
		PBEKeySpec spec = new PBEKeySpec(password.toCharArray(), salt, iteration) ;		 
		return spec ;
	}
	private byte[] encryptKey(byte[] data, PBEKeySpec keyspec) throws Throwable {
		SecretKey key = getPasswordKey(keyspec) ;
		PBEParameterSpec paramSpec = new PBEParameterSpec(keyspec.getSalt(), keyspec.getIterationCount()) ;
		Cipher c = Cipher.getInstance(key.getAlgorithm()) ;
		c.init(Cipher.ENCRYPT_MODE, key,paramSpec);
		byte[] encrypted = c.doFinal(data) ;
		 
		return encrypted ;
	}
	private SecretKey getPasswordKey(PBEKeySpec keyspec) throws Throwable {
		SecretKeyFactory factory = SecretKeyFactory.getInstance(PBE_ALGO) ;
		SecretKey PbKey = factory.generateSecret(keyspec) ;
		return PbKey ;
	}
	private byte[] decryptKey(byte[] encrypted, PBEKeySpec keyspec) throws Throwable {
		SecretKey key = getPasswordKey(keyspec) ;
		PBEParameterSpec paramSpec = new PBEParameterSpec(keyspec.getSalt(), keyspec.getIterationCount()) ;
		Cipher c = Cipher.getInstance(key.getAlgorithm()) ;
		c.init(Cipher.DECRYPT_MODE, key, paramSpec);
		byte[] data = c.doFinal(encrypted) ;
		return data ;
	}
	private SecretKey getMasterKeyFromBytes(byte[] keyData) throws Throwable {
		SecretKeySpec sks = new SecretKeySpec(keyData, MK_CIPHER) ;
		return sks ;
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
