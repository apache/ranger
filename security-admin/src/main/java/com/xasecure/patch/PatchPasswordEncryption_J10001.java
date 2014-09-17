package com.xasecure.patch;

import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.xasecure.common.StringUtil;
import com.xasecure.db.XADaoManager;
import com.xasecure.entity.XXAsset;
import com.xasecure.service.XAssetService;
import com.xasecure.util.CLIUtil;

@Component
public class PatchPasswordEncryption_J10001 extends BaseLoader {
	static Logger logger = Logger.getLogger(PatchPasswordEncryption_J10001.class);
	int lineCount = 0;
	
	@Autowired
	XADaoManager xaDaoManager;
	
	@Autowired
	StringUtil stringUtil;
	
	@Autowired
	XAssetService xAssetService;
	
	public PatchPasswordEncryption_J10001() {
	}
	

	@Override
	public void printStats() {
		logger.info("Time taken so far:" + timeTakenSoFar(lineCount)
				+ ", moreToProcess=" + isMoreToProcess());
		print(lineCount, "Processed lines");
	}

	@Override
	public void execLoad() {
		encryptLookupUserPassword();
	}

	private void encryptLookupUserPassword() {
		List<XXAsset> xAssetList = xaDaoManager.getXXAsset().getAll();
		String oldConfig=null;
		String newConfig=null;
		for (XXAsset xAsset : xAssetList) {		
			oldConfig=null;
			newConfig=null;
			oldConfig=xAsset.getConfig();
			if(!stringUtil.isEmpty(oldConfig)){
				newConfig=xAssetService.getConfigWithEncryptedPassword(oldConfig,false);
				xAsset.setConfig(newConfig);
				xaDaoManager.getXXAsset().update(xAsset);
			}
			lineCount++;
			logger.info("Lookup Password updated for Asset : "
					+ xAsset.getName());
			logger.info("oldconfig : "+ oldConfig);
			logger.info("newConfig : "+ newConfig);
			print(lineCount, "Total updated assets count : ");
		}
	}

	public static void main(String[] args) {
		logger.info("main()");
		try {
			PatchPasswordEncryption_J10001 loader = (PatchPasswordEncryption_J10001) CLIUtil
					.getBean(PatchPasswordEncryption_J10001.class);
			//loader.init();
			while (loader.isMoreToProcess()) {
				loader.load();
			}
			logger.info("Load complete. Exiting!!!");
			System.exit(0);
		}catch (Exception e) {
			logger.error("Error loading", e);
			System.exit(1);
		}
	}

}
