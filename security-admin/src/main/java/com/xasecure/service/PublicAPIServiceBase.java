package com.xasecure.service;

import com.xasecure.view.VXDataObject;

public abstract class PublicAPIServiceBase<VXA extends VXDataObject, VX extends VXDataObject> {

	protected VX mapBaseAttributesToPublicObject(VXA vXAObj, VX vPublicObj) {
		vPublicObj.setId(vXAObj.getId());
		vPublicObj.setCreateDate(vXAObj.getCreateDate());
		vPublicObj.setUpdateDate(vXAObj.getUpdateDate());
		vPublicObj.setOwner(vXAObj.getOwner());
		vPublicObj.setUpdatedBy(vXAObj.getUpdatedBy());
		return vPublicObj;
	}

	protected VXA mapBaseAttributesToXAObject(VX vPublicObj, VXA vXAObj) {
		vXAObj.setId(vPublicObj.getId());
		vXAObj.setCreateDate(vPublicObj.getCreateDate());
		vXAObj.setUpdateDate(vPublicObj.getUpdateDate());
		vXAObj.setOwner(vPublicObj.getOwner());
		vXAObj.setUpdatedBy(vPublicObj.getUpdatedBy());

		return vXAObj;
	}

}
