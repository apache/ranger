package org.apache.ranger.service;

import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.SearchField;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXGroup;
import org.apache.ranger.entity.XXGroupPermission;
import org.apache.ranger.view.VXGroupPermission;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

@Service
@Scope("singleton")
public class XGroupPermissionService extends XGroupPermissionServiceBase<XXGroupPermission, VXGroupPermission>{

	public static Long createdByUserId = 1L;

	@Autowired
	RangerDaoManager rangerDaoManager;

	public XGroupPermissionService() {
		searchFields.add(new SearchField("id", "obj.id",
				SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.FULL));

		searchFields.add(new SearchField("groupPermissionList", "obj.groupId",
				SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL,
				"XXModuleDef xXModuleDef", "xXModuleDef.id = obj.groupId "));
	}

	@Override
	protected void validateForCreate(VXGroupPermission vObj) {

	}

	@Override
	protected void validateForUpdate(VXGroupPermission vObj, XXGroupPermission mObj) {

	}

	@Override
	public VXGroupPermission populateViewBean(XXGroupPermission xObj) {
		VXGroupPermission vObj = super.populateViewBean(xObj);
		XXGroup xGroup = rangerDaoManager.getXXGroup().getById(
				xObj.getGroupId());

		if (xGroup == null) {
			throw restErrorUtil.createRESTException(xGroup + " is Not Found",
					MessageEnums.DATA_NOT_FOUND);
		}

		vObj.setGroupName(xGroup.getName());
		return vObj;
	}
}
