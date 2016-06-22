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
package org.apache.ranger.plugin.util;

import java.io.IOException;
import java.util.Map;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.PBEParameterSpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jersey.core.util.Base64;
public class PasswordUtils {

	private static final Logger LOG = LoggerFactory.getLogger(PasswordUtils.class) ;
	
	private static final char[] ENCRYPT_KEY = "tzL1AKl5uc4NKYaoQ4P3WLGIBFPXWPWdu1fRm9004jtQiV".toCharArray() ;
	
	private static final byte[] SALT = "f77aLYLo".getBytes() ;
	
	private static final int ITERATION_COUNT = 17 ;
	
	private static final String CRYPT_ALGO = "PBEWithMD5AndDES" ;
	
	private static final String PBE_KEY_ALGO = "PBEWithMD5AndDES" ;
	
	private static final String LEN_SEPARATOR_STR = ":" ;		
	
	public static String encryptPassword(String aPassword) throws IOException {
		Map<String, String> env = System.getenv();
		String encryptKeyStr = env.get("ENCRYPT_KEY") ;
		char[] encryptKey;		
		if (encryptKeyStr == null) {
			encryptKey=ENCRYPT_KEY;
		}else{
			encryptKey=encryptKeyStr.toCharArray();
		}
		String saltStr = env.get("ENCRYPT_SALT") ;
		byte[] salt;
		if (saltStr == null) {
			salt = SALT ;
		}else{
			salt=saltStr.getBytes();
		}
		String ret = null ;
		String strToEncrypt = null ;		
		if (aPassword == null) {
			strToEncrypt = "" ;
		}
		else {
			strToEncrypt = aPassword.length() + LEN_SEPARATOR_STR + aPassword ;
		}		
		try {
			Cipher engine = Cipher.getInstance(CRYPT_ALGO) ;
			PBEKeySpec keySpec = new PBEKeySpec(encryptKey) ;
			SecretKeyFactory skf = SecretKeyFactory.getInstance(PBE_KEY_ALGO) ;
			SecretKey key = skf.generateSecret(keySpec) ;
			engine.init(Cipher.ENCRYPT_MODE, key, new PBEParameterSpec(salt, ITERATION_COUNT));
			byte[] encryptedStr = engine.doFinal(strToEncrypt.getBytes()) ;
			ret = new String(Base64.encode(encryptedStr)) ;
		}
		catch(Throwable t) {
			LOG.error("Unable to encrypt password due to error", t);
			throw new IOException("Unable to encrypt password due to error", t) ;
		}		
		return ret ;
	}

	public static String decryptPassword(String aPassword) throws IOException {
		String ret = null ;
		Map<String, String> env = System.getenv();
		String encryptKeyStr = env.get("ENCRYPT_KEY") ;
		char[] encryptKey;		
		if (encryptKeyStr == null) {
			encryptKey=ENCRYPT_KEY;
		}else{
			encryptKey=encryptKeyStr.toCharArray();
		}
		String saltStr = env.get("ENCRYPT_SALT") ;
		byte[] salt;
		if (saltStr == null) {
			salt = SALT ;
		}else{
			salt=saltStr.getBytes();
		}
		try {			
			byte[] decodedPassword = Base64.decode(aPassword) ;
			Cipher engine = Cipher.getInstance(CRYPT_ALGO) ;
			PBEKeySpec keySpec = new PBEKeySpec(encryptKey) ;
			SecretKeyFactory skf = SecretKeyFactory.getInstance(PBE_KEY_ALGO) ;
			SecretKey key = skf.generateSecret(keySpec) ;
			engine.init(Cipher.DECRYPT_MODE, key,new PBEParameterSpec(salt, ITERATION_COUNT));
			String decrypted = new String(engine.doFinal(decodedPassword)) ;
			int foundAt = decrypted.indexOf(LEN_SEPARATOR_STR) ;
			if (foundAt > -1) {
				if (decrypted.length() > foundAt) {
					ret = decrypted.substring(foundAt+1) ;
				}
				else {
					ret = "" ;
				}
			}
			else {
				ret = null;
			}
		}
		catch(Throwable t) {
			LOG.error("Unable to decrypt password due to error", t);
			throw new IOException("Unable to decrypt password due to error", t) ;
		}
		return ret ;
	}
	
	public static void main(String[] args) {		
		String[] testPasswords = { "a", "a123", "dsfdsgdg", "*7263^5#", "", null } ;		
		for(String password : testPasswords) {
			try {
				String ePassword = PasswordUtils.encryptPassword(password) ;
				String dPassword = PasswordUtils.decryptPassword(ePassword) ;
				if (password == null ) {
					if (dPassword != null) {
						throw new RuntimeException("The password expected [" + password + "]. Found [" + dPassword + "]") ;
					}
					else {
						System.out.println("Password: [" + password + "] matched after decrypt. Encrypted: [" + ePassword + "]") ;
					}
				}
				else if (! password.equals(dPassword)) {
					throw new RuntimeException("The password expected [" + password + "]. Found [" + dPassword + "]") ;
				}
				else {
					System.out.println("Password: [" + password + "] matched after decrypt. Encrypted: [" + ePassword + "]") ;
				}
			}
			catch(IOException ioe) {
				ioe.printStackTrace(); 
				System.out.println("Password verification failed for password [" + password + "]:" + ioe) ;
			}			
		}		
	}
}
