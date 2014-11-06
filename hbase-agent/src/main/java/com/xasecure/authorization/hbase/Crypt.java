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

package com.xasecure.authorization.hbase;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Crypt {
	
	private static final Log LOG = LogFactory.getLog("SecurityLogger." + XaSecureAuthorizationCoprocessor.class.getName());

	private static byte[] IV = { 01, 21, 22, 86, 121, 45, 98, 28, 111, 72, 54, 39, 96, 47, 84, 13 };
	private static final byte[] encryptionKey = "324234sdff3a7d8e".getBytes();
	private static final String CIPHER_ALGO = "AES/CBC/PKCS5Padding";
	private static final String CIPHER_INIT_ALGO = "AES";
	
	private static Crypt me = null ;
	
	private Cipher encrypter = null;
	private Cipher descrypter = null;


	public static Crypt getInstance() {
		if (me == null) {
			synchronized (Crypt.class) {
				Crypt other = me ;
				if (other == null) {
					me = new Crypt() ;
				}
			}
		}
		return me ;
	}
	
	private Crypt() {
		try {
			encrypter = Cipher.getInstance(CIPHER_ALGO);
			SecretKeySpec enckey = new SecretKeySpec(encryptionKey, CIPHER_INIT_ALGO);
			encrypter.init(Cipher.ENCRYPT_MODE, enckey, new IvParameterSpec(IV));

			descrypter = Cipher.getInstance(CIPHER_ALGO);
			SecretKeySpec deckey = new SecretKeySpec(encryptionKey, CIPHER_INIT_ALGO);
			descrypter.init(Cipher.DECRYPT_MODE, deckey, new IvParameterSpec(IV));
		} catch (Throwable t) {
			LOG.error("Unable to initialzie Encrypt/Decrypt module - Exiting from HBase", t);
			System.exit(1);
		}
	}
	
	public synchronized byte[] encrypt(byte[] plainText) throws Exception {
		byte[] ret =  encrypter.doFinal(plainText);
		LOG.debug("Encrypted plain text: [" + new String(plainText) + "] => {" +  Hex.encodeHexString(ret)  + "}") ;
		return ret ;
	}

	public synchronized byte[] decrypt(byte[] cipherText) throws Exception {
		byte[] ret =  descrypter.doFinal(cipherText);
		LOG.debug("Decrypted From text: [" + Hex.encodeHexString(cipherText)   + "] => {" +  new String(ret)   + "}") ;
		return ret ;
	}


}
