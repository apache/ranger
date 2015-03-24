package org.apache.ranger.kms.dao;

import javax.persistence.EntityManager;
import org.apache.log4j.Logger;

public abstract class DaoManagerBase {
	final static Logger logger = Logger.getLogger(DaoManagerBase.class);

	abstract public EntityManager getEntityManager();

	private RangerMasterKeyDao rangerMasterKeyDao = null;
	private RangerKMSDao rangerKmsDao = null;

    public DaoManagerBase() {
	}

	public RangerMasterKeyDao getRangerMasterKeyDao() {
		if(rangerMasterKeyDao == null) {
			rangerMasterKeyDao = new RangerMasterKeyDao(this);
		}

		return rangerMasterKeyDao;
	}
	
	public RangerKMSDao getRangerKMSDao(){
		if(rangerKmsDao == null){
			rangerKmsDao = new RangerKMSDao(this);
		}
		return rangerKmsDao;
	}
}
