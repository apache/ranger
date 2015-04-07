package org.apache.ranger.service;

import java.util.ArrayList;
import java.util.List;
import org.apache.ranger.common.SearchCriteria;
import org.apache.ranger.entity.XXGroupPermission;
import org.apache.ranger.view.VXGroupPermission;
import org.apache.ranger.view.VXGroupPermissionList;

public abstract class XGroupPermissionServiceBase<T extends XXGroupPermission, V extends VXGroupPermission>
		extends AbstractBaseResourceService<T, V> {

	public static final String NAME = "XGroupPermission";

	public XGroupPermissionServiceBase() {

	}

	@SuppressWarnings("unchecked")
	@Override
	protected XXGroupPermission mapViewToEntityBean(VXGroupPermission vObj,
			XXGroupPermission mObj, int OPERATION_CONTEXT) {
		mObj.setGroupId(vObj.getGroupId());
		mObj.setModuleId(vObj.getModuleId());
		mObj.setIsAllowed(vObj.getIsAllowed());
		return mObj;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected VXGroupPermission mapEntityToViewBean(VXGroupPermission vObj, XXGroupPermission mObj) {
		vObj.setGroupId(mObj.getGroupId());
		vObj.setModuleId(mObj.getModuleId());
		vObj.setIsAllowed(mObj.getIsAllowed());
		return vObj;
	}

	/**
	 * @param searchCriteria
	 * @return
	 */
	public VXGroupPermissionList searchXGroupPermission(SearchCriteria searchCriteria) {
		VXGroupPermissionList returnList = new VXGroupPermissionList();
		List<VXGroupPermission> vXGroupPermissions = new ArrayList<VXGroupPermission>();

		@SuppressWarnings("unchecked")
		List<XXGroupPermission> resultList = (List<XXGroupPermission>) searchResources(
				searchCriteria, searchFields, sortFields, returnList);

		// Iterate over the result list and create the return list
		for (XXGroupPermission gjXUser : resultList) {
			@SuppressWarnings("unchecked")
			VXGroupPermission vXGroupPermission = populateViewBean((T) gjXUser);
			vXGroupPermissions.add(vXGroupPermission);
		}

		returnList.setvXGroupPermission(vXGroupPermissions);
		return returnList;
	}
}
