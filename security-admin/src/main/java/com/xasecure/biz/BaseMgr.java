/**
 *
 */
package com.xasecure.biz;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.xasecure.common.XAConstants;
import com.xasecure.common.MessageEnums;
import com.xasecure.common.RESTErrorUtil;
import com.xasecure.common.db.BaseDao;
import com.xasecure.db.XADaoManager;
import com.xasecure.entity.XXDBBase;

@Component
public abstract class BaseMgr {
    static final Logger logger = Logger.getLogger(BaseMgr.class);

    @Autowired
    XADaoManager daoManager;

    @Autowired
    RESTErrorUtil restErrorUtil;

    public XADaoManager getDaoManager() {
	return daoManager;
    }

    public void deleteEntity(BaseDao<? extends XXDBBase> baseDao, Long id,
	    String entityName) {
	XXDBBase entity = baseDao.getById(id);
	if (entity != null) {
	    try {
		baseDao.remove(id);
	    } catch (Exception e) {
		logger.error("Error deleting " + entityName + ". Id=" + id, e);
		throw restErrorUtil.createRESTException("This " + entityName
			+ " can't be deleted",
			MessageEnums.OPER_NOT_ALLOWED_FOR_STATE, id, null, ""
				+ id + ", error=" + e.getMessage());
	    }
	} else {
	    // Return without error
	    logger.info("Delete ignored for non-existent " + entityName
		    + " id=" + id);
	}
    }

    /**
     * @param objectClassType
     */
    protected void validateClassType(int objectClassType) {
	// objectClassType
	restErrorUtil.validateMinMax(objectClassType, 1,
		XAConstants.ClassTypes_MAX, "Invalid classType", null,
		"objectClassType");
    }

}
