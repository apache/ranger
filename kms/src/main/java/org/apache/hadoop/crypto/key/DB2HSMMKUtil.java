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

import java.io.Console;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

import org.apache.hadoop.conf.Configuration;
import org.apache.ranger.kms.dao.DaoManager;

import com.sun.org.apache.xml.internal.security.utils.Base64;

public class DB2HSMMKUtil {

	private static final String ENCRYPTION_KEY = "ranger.db.encrypt.key.password" ;
	private static final String PARTITION_PASSWORD = "ranger.ks.hsm.partition.password";
	private static final String PARTITION_NAME = "ranger.ks.hsm.partition.name";
	private static final String HSM_TYPE = "ranger.ks.hsm.type";
	
	public static void showUsage() {
		System.err.println("USAGE: java " + DB2HSMMKUtil.class.getName() + " <HSMType> <partitionName>") ;
	}
	
	public static void main(String[] args) {
			if (args.length < 2) {
				System.err.println("Invalid number of parameters found.") ;
				showUsage() ;
				System.exit(1) ;
			}
			else {				
				String hsmType = args[0];
				if (hsmType == null || hsmType.trim().isEmpty()) {
					System.err.println("HSM Type does not exists.") ;
					showUsage() ;
					System.exit(1);
				}
				
				String partitionName = args[1];
				if (partitionName == null || partitionName.trim().isEmpty()) {
					System.err.println("Partition name does not exists.") ;
					showUsage() ;
					System.exit(1) ;
				}
				
				boolean result = new DB2HSMMKUtil().doExportMKToHSM(hsmType, partitionName);
				if(result){
					System.out.println("Master Key from Ranger KMS DB has been successfully imported into HSM.") ;
				}else{
					System.out.println("Import of Master Key from DB has been unsuccessful.") ;
				}
				System.exit(0) ;
				
			}
	}
	
	private boolean doExportMKToHSM(String hsmType, String partitionName) {
		try {
			String partitionPassword = getPasswordFromConsole("Enter Password for the Partition "+partitionName+" : ") ;
			Configuration conf = RangerKeyStoreProvider.getDBKSConf();
			conf.set(HSM_TYPE, hsmType);
			conf.set(PARTITION_NAME, partitionName);
			conf.set(PARTITION_PASSWORD, partitionPassword);
			
			RangerKMSDB rangerkmsDb = new RangerKMSDB(conf);		
			DaoManager daoManager = rangerkmsDb.getDaoManager();
			String password = conf.get(ENCRYPTION_KEY);
			
			// Get Master Key from Ranger DB			
			RangerMasterKey rangerMasterKey = new RangerMasterKey(daoManager);
			String mkey = rangerMasterKey.getMasterKey(password);
			byte[] key = Base64.decode(mkey);
			
			// Put Master Key in HSM
			RangerHSM rangerHSM = new RangerHSM(conf);
			
			return rangerHSM.setMasterKey(password, key);
		}
		catch(Throwable t) {
			throw new RuntimeException("Unable to import Master key from Ranger DB to HSM ", t) ;
		}
	}
		
	private String getPasswordFromConsole(String prompt) throws IOException {
		String ret = null ;
		Console c=System.console();
	    if (c == null) {
	        System.out.print(prompt + " ");
	        InputStream in=System.in;
	        int max=50;
	        byte[] b=new byte[max];
	        int l= in.read(b);
	        l--;     //last character is \n
	        if (l>0) {
	            byte[] e=new byte[l];
	            System.arraycopy(b,0, e, 0, l);
	            ret = new String(e, Charset.defaultCharset());
	        } 
	    } else { 
	    	char[] pwd = c.readPassword(prompt + " ") ;
	    	if (pwd == null) {
	    		ret = null ;
	    	}
	    	else {
	    		ret = new String(pwd);
	    	}
	    }
	    if (ret == null) {
	    	ret = "" ;
	    }
	    return ret;
	}	
}
