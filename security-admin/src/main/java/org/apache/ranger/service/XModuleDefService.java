package org.apache.ranger.service;

import java.util.ArrayList;
import java.util.List;

import org.apache.ranger.common.RangerConstants;
import org.apache.ranger.common.SearchField;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXGroupPermission;
import org.apache.ranger.entity.XXModuleDef;
import org.apache.ranger.entity.XXUserPermission;
import org.apache.ranger.view.VXGroupPermission;
import org.apache.ranger.view.VXModuleDef;
import org.apache.ranger.view.VXUserPermission;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

@Service
@Scope("singleton")
public class XModuleDefService extends
		XModuleDefServiceBase<XXModuleDef, VXModuleDef> {

	public static Long createdByUserId = 1L;

	@Autowired
	RangerDaoManager rangerDaoManager;

	@Autowired
	XUserPermissionService xUserPermService;

	@Autowired
	XGroupPermissionService xGrpPermService;

	public XModuleDefService(){
		searchFields.add(new SearchField("module", "obj.module",
            SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.PARTIAL));
searchFields
            .add(new SearchField(
                            "userName",
                            "portalUser.loginId",
                            SearchField.DATA_TYPE.STRING,
                            SearchField.SEARCH_TYPE.PARTIAL,
                            " XXPortalUser portalUser,  XXUserPermission userPermission",
                            "obj.id=userPermission.moduleId and portalUser.id=userPermission.userId and userPermission.isAllowed="
                                            + RangerConstants.IS_ALLOWED));
searchFields
            .add(new SearchField(
                            "groupName",
                            "group.name",
                            SearchField.DATA_TYPE.STRING,
                            SearchField.SEARCH_TYPE.PARTIAL,
                            "XXGroup group,XXGroupPermission groupModulePermission",
                            "obj.id=groupModulePermission.moduleId and groupModulePermission.groupId=group.id and groupModulePermission.isAllowed="));
}

	@Override
	protected void validateForCreate(VXModuleDef vObj) {

	}

	@Override
	protected void validateForUpdate(VXModuleDef vObj, XXModuleDef mObj) {

	}

	@Override
	public VXModuleDef populateViewBean(XXModuleDef xObj) {

		VXModuleDef vModuleDef = super.populateViewBean(xObj);
		List<VXUserPermission> vXUserPermissionList = new ArrayList<VXUserPermission>();
		List<VXGroupPermission> vXGroupPermissionList = new ArrayList<VXGroupPermission>();

		List<XXUserPermission> xuserPermissionList = rangerDaoManager
				.getXXUserPermission().findByModuleId(xObj.getId(), false);
		List<XXGroupPermission> xgroupPermissionList = rangerDaoManager
				.getXXGroupPermission().findByModuleId(xObj.getId(), false);
		for (XXUserPermission xUserPerm : xuserPermissionList) {

			VXUserPermission vXUserPerm = xUserPermService
					.populateViewBean(xUserPerm);
			vXUserPermissionList.add(vXUserPerm);

		}

		for (XXGroupPermission xGrpPerm : xgroupPermissionList) {

			VXGroupPermission vXGrpPerm = xGrpPermService
					.populateViewBean(xGrpPerm);
			vXGroupPermissionList.add(vXGrpPerm);

		}

		vModuleDef.setUserPermList(vXUserPermissionList);
		vModuleDef.setGroupPermList(vXGroupPermissionList);
		return vModuleDef;
	}

}
