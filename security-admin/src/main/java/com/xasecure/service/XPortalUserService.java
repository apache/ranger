package com.xasecure.service;

import com.xasecure.entity.*;
import com.xasecure.view.*;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

@Service
@Scope("singleton")
public class XPortalUserService extends XPortalUserServiceBase<XXPortalUser, VXPortalUser> {

	@Override
	protected void validateForCreate(VXPortalUser vObj) {
		// TODO Auto-generated method stub

	}

	@Override
	protected void validateForUpdate(VXPortalUser vObj, XXPortalUser mObj) {
		// TODO Auto-generated method stub

	}

}
