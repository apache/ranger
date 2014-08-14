package com.xasecure.authorization.hadoop.utils;

import java.lang.String;
import java.util.List;
import java.io.IOException;

import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class XaSecureCredentialProvider {

  private static Log LOG = LogFactory.getLog(XaSecureCredentialProvider.class);
  private static Configuration conf = null;

  private static List<CredentialProvider> providers = null;
  private static XaSecureCredentialProvider  me = null;

  
  public static XaSecureCredentialProvider getInstance()  {
	  if ( me == null) {
		  synchronized(XaSecureCredentialProvider.class) {
			  XaSecureCredentialProvider temp = me;
			  if ( temp == null){
				  me = new XaSecureCredentialProvider();
				  me.init();
			  }
		  }
	  }
	return me;
  }
  
  
  private void init() {
	  conf  = new Configuration();
  }
  
  public char[] getCredentialString(String url, String alias)  {
  
   char[] pass = null;
 
   conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, url);
   providers =  getCredentialProviders(); 
   
   CredentialProvider.CredentialEntry credEntry = null;
   
   for(  CredentialProvider provider: providers) {
	   try {
         credEntry = provider.getCredentialEntry(alias);
         if (credEntry != null) {
            pass = credEntry.getCredential();
         } else {
        	return pass;
         }
        } catch(IOException ie) {
        	LOG.error("Unable to get the Credential Provider from the Configuration", ie);	 
       }
    }
   return pass;
  }
  
  public  List<CredentialProvider>  getCredentialProviders(){
   try {
       providers = CredentialProviderFactory.getProviders(conf);   
      } catch( IOException ie) {
    	  LOG.error("Unable to get the Credential Provider from the Configuration", ie);
      }     
   return providers;
  }

}