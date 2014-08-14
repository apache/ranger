/**
 * 
 */
package com.xasecure.biz;

import org.apache.log4j.Logger;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import com.xasecure.hive.client.HiveClient;

/**
 * 
 */

// @Component
// @Scope("singleton")

public class HiveFSMgr {

	private HiveClient fs;
	private String dataSource;
	private static Logger logger = Logger.getLogger(HiveFSMgr.class);

	public HiveFSMgr() {
		init();
	}

	public HiveFSMgr(String dataSource) {
		this.dataSource = dataSource;
		init();
	}

	private void init() {
		try {
			if (dataSource != null) {
				fs = new HiveClient(dataSource);
			} else {
				fs = new HiveClient("dev-hive");
			}
		} catch (Exception e) {
			logger.error("Error connecting hive client", e);
		}
	}

	protected HiveClient getInstance(String dataSourceName) {
		if (dataSourceName == null) {
			logger.info("Hive client name not provided.");
			return fs;
		} else {
			if (fs.getDataSource() != null) {
				if (fs.getDataSource().equalsIgnoreCase(dataSourceName)) {
					return fs;
				} else {
					fs = new HiveClient(dataSourceName);
					return fs;
				}
			} else {
				fs = new HiveClient(dataSourceName);
				return fs;
			}
		}
	}

}
