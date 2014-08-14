/**
 * 
 */
package com.xasecure.biz;

import org.apache.log4j.Logger;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.xasecure.hadoop.client.HadoopFS;

/**
 * 
 * 
 */

@Component
@Scope("singleton")
public class HadoopFSMgr {

	private HadoopFS fs;
	private String dataSource;
	private static Logger logger = Logger.getLogger(HadoopFSMgr.class);

	public HadoopFSMgr() {
		init();
	}

	public HadoopFSMgr(String dataSource) {
		this.dataSource = dataSource;
		init();
	}

	private void init() {
		try {
//			if (dataSource != null) {
//				fs = new HadoopFS(dataSource);
//			} else {
//				fs = new HadoopFS("hadoopdev");
//			}
		} catch (Exception e) {
			logger.error("Error connecting hive client", e);
		}
	}

	protected HadoopFS getInstance(String dataSourceName) {
		if (dataSourceName == null) {
			logger.info("Hadoop client name not provided.");
			return fs;
		} else {
			if (fs.getDataSource() != null) {
				if (fs.getDataSource().equalsIgnoreCase(dataSourceName)) {
					return fs;
				} else {
					fs = new HadoopFS(dataSourceName);
					return fs;
				}
			} else {
				fs = new HadoopFS(dataSourceName);
				return fs;
			}
		}
	}

}
