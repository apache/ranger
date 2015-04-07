package org.apache.ranger.db;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.NoResultException;

import org.apache.log4j.Logger;
import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.entity.XXModuleDef;

public class XXModuleDefDao extends BaseDao<XXModuleDef>{

	static final Logger logger = Logger.getLogger(XXModuleDefDao.class);

	public XXModuleDefDao(RangerDaoManagerBase daoManager) {
		super(daoManager);
	}

	public XXModuleDef findByModuleName(String moduleName){
		if (moduleName == null) {
			return null;
		}
		try {

			return (XXModuleDef) getEntityManager()
					.createNamedQuery("XXModuleDef.findByModuleName")
					.setParameter("moduleName", moduleName)
					.getSingleResult();
		} catch (Exception e) {

		}
		return null;
	}


	public XXModuleDef  findByModuleId(Long id) {
		if(id == null) {
			return new XXModuleDef();
		}
		try {
			List<XXModuleDef> xxModuelDefs=getEntityManager()
					.createNamedQuery("XXModuleDef.findByModuleId", tClass)
					.setParameter("id", id).getResultList();
			return xxModuelDefs.get(0);
		} catch (NoResultException e) {
			return new XXModuleDef();
		}
	}
	@SuppressWarnings("unchecked")
	public List<XXModuleDef>  findModuleNamesWithIds() {
		try {
			return getEntityManager()
					.createNamedQuery("XXModuleDef.findModuleNamesWithIds")
					.getResultList();
		} catch (NoResultException e) {
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	public List<String>  findModuleURLOfPemittedModules(Long userId) {
		try {

			String query="select";
			query+=" url";
			query+=" FROM";
			query+="   x_modules_master";
			query+=" WHERE";
			query+="  url NOT IN (SELECT ";
			query+="    moduleMaster.url";
			query+=" FROM";
			query+=" x_modules_master moduleMaster,";
			query+=" x_user_module_perm userModulePermission";
			query+=" WHERE";
			query+=" moduleMaster.id = userModulePermission.module_id";
			query+=" AND userModulePermission.user_id = "+userId+")";
			query+=" AND ";
			query+=" id NOT IN (SELECT DISTINCT";
			query+=" gmp.module_id";
			query+=" FROM";
			query+=" x_group_users xgu,";
			query+=" x_user xu,";
			query+=" x_group_module_perm gmp,";
			query+=" x_portal_user xpu";
			query+=" WHERE";
			query+=" xu.user_name = xpu.login_id";
			query+=" AND xu.id = xgu.user_id";
			query+=" AND xgu.p_group_id = gmp.group_id";
			query+=" AND xpu.id = "+userId+")";

			return getEntityManager()
					.createNativeQuery(query)
					.getResultList();

		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
}
