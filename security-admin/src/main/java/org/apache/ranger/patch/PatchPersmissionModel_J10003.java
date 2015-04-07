package org.apache.ranger.patch;

import org.apache.log4j.Logger;
import org.apache.ranger.biz.XUserMgr;
import org.apache.ranger.util.CLIUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
@Component
public class PatchPersmissionModel_J10003 extends BaseLoader {
	private static Logger logger = Logger.getLogger(PatchPersmissionModel_J10003.class);

	@Autowired
	XUserMgr xUserMgr;

	public static void main(String[] args) {
		logger.info("main()");
		try {
			PatchPersmissionModel_J10003 loader = (PatchPersmissionModel_J10003) CLIUtil.getBean(PatchPersmissionModel_J10003.class);
			loader.init();
			while (loader.isMoreToProcess()) {
				loader.load();
			}
			logger.info("Load complete. Exiting!!!");
			System.exit(0);
		} catch (Exception e) {
			logger.error("Error loading", e);
			System.exit(1);
		}
	}

	@Override
	public void init() throws Exception {
		// Do Nothing
	}

	@Override
	public void execLoad() {
		logger.info("==> PermissionPatch.execLoad()");
		try {
			xUserMgr.updateExistingUserExisting();
		} catch (Exception e) {
			logger.error("Error whille migrating data.", e);
		}
		logger.info("<== PermissionPatch.execLoad()");
	}

	@Override
	public void printStats() {
	}
}
