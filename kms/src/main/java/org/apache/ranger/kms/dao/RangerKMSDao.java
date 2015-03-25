package org.apache.ranger.kms.dao;

import org.apache.ranger.entity.XXRangerKeyStore;

public class RangerKMSDao extends BaseDao<XXRangerKeyStore> {

	public RangerKMSDao(DaoManagerBase daoManager) {
		super(daoManager);
	}
	
	public XXRangerKeyStore findByAlias(String alias){
		return super.findByAlias("XXRangerKeyStore.findByAlias", alias);
	}
	
	public int deleteByAlias(String alias){
		return super.deleteByAlias("XXRangerKeyStore.deleteByAlias", alias);
	}
}
