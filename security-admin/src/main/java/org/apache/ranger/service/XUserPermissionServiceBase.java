package org.apache.ranger.service;

import java.util.ArrayList;
import java.util.List;

import org.apache.ranger.common.SearchCriteria;
import org.apache.ranger.entity.XXUserPermission;
import org.apache.ranger.view.VXUserPermission;
import org.apache.ranger.view.VXUserPermissionList;

public abstract class XUserPermissionServiceBase<T extends XXUserPermission, V extends VXUserPermission>
		extends AbstractBaseResourceService<T, V> {

	public static final String NAME = "XUserPermission";

	public XUserPermissionServiceBase() {

	}

	@SuppressWarnings("unchecked")
	@Override
	protected XXUserPermission mapViewToEntityBean(VXUserPermission vObj,
			XXUserPermission mObj, int OPERATION_CONTEXT) {
		mObj.setUserId(vObj.getUserId());
		mObj.setModuleId(vObj.getModuleId());
		mObj.setIsAllowed(vObj.getIsAllowed());
		return mObj;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected VXUserPermission mapEntityToViewBean(VXUserPermission vObj, XXUserPermission mObj) {
		vObj.setUserId(mObj.getUserId());
		vObj.setModuleId(mObj.getModuleId());
		vObj.setIsAllowed(mObj.getIsAllowed());
		return vObj;
	}

	/**
	 * @param searchCriteria
	 * @return
	 */
	public VXUserPermissionList searchXUserPermission(SearchCriteria searchCriteria) {
		VXUserPermissionList returnList = new VXUserPermissionList();
		List<VXUserPermission> vXUserPermissions = new ArrayList<VXUserPermission>();

		@SuppressWarnings("unchecked")
		List<XXUserPermission> resultList = (List<XXUserPermission>) searchResources(
				searchCriteria, searchFields, sortFields, returnList);

		// Iterate over the result list and create the return list
		for (XXUserPermission gjXUser : resultList) {
			@SuppressWarnings("unchecked")
			VXUserPermission vXUserPermission = populateViewBean((T) gjXUser);
			vXUserPermissions.add(vXUserPermission);
		}

		returnList.setvXModuleDef(vXUserPermissions);
		return returnList;
	}
}