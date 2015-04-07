package org.apache.ranger.service;

import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.SearchField;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXPortalUser;
import org.apache.ranger.entity.XXUser;
import org.apache.ranger.entity.XXUserPermission;
import org.apache.ranger.view.VXUserPermission;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;
@Service
@Scope("singleton")
public class XUserPermissionService extends XUserPermissionServiceBase<XXUserPermission, VXUserPermission>{

	public static Long createdByUserId = 1L;

	@Autowired
	RangerDaoManager rangerDaoManager;

	public XUserPermissionService() {
		searchFields.add(new SearchField("id", "obj.id",
				SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.FULL));

		searchFields.add(new SearchField("userPermissionList", "obj.userId",
				SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL,
				"XXModuleDef xXModuleDef", "xXModuleDef.id = obj.userId "));
	}

	@Override
	protected void validateForCreate(VXUserPermission vObj) {

	}

	@Override
	protected void validateForUpdate(VXUserPermission vObj, XXUserPermission mObj) {

	}

	@Override
	public VXUserPermission populateViewBean(XXUserPermission xObj) {
		VXUserPermission vObj = super.populateViewBean(xObj);

		XXPortalUser xUser = rangerDaoManager.getXXPortalUser().getById(xObj.getUserId());
		if (xUser == null) {
			xUser=rangerDaoManager.getXXPortalUser().findByXUserId(xObj.getUserId());
			if(xUser==null)
			throw restErrorUtil.createRESTException(xUser + " is Not Found",
					MessageEnums.DATA_NOT_FOUND);
		}

		vObj.setUserName(xUser.getLoginId());
		return vObj;
	}

}