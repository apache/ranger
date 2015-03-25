package org.apache.ranger.kms.biz;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServlet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.RangerKeyStoreProvider;
import org.apache.hadoop.crypto.key.RangerMasterKey;
import org.springframework.stereotype.Component;

@SuppressWarnings("serial")
@Component
public class RangerKMSStartUp extends HttpServlet
{	
	public static final String ENCRYPTION_KEY = "ranger.db.encrypt.key.password";
	private static Logger LOG = LoggerFactory.getLogger(RangerKMSStartUp.class);
	
	@PostConstruct
	public void initRangerMasterKey() {
		LOG.info("Ranger KMSStartUp");
		RangerMasterKey rangerMasterKey = new RangerMasterKey();
		try {
				Configuration conf = RangerKeyStoreProvider.getDBKSConf();
				String password = conf.get(ENCRYPTION_KEY);
				boolean check = rangerMasterKey.generateMasterKey(password);
				if(check){
					LOG.info("MasterKey Generated..");
				}
		} catch (Throwable e) {
			e.printStackTrace();
		}				
	}	
}
