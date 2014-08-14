package com.xasecure.service;

import com.xasecure.entity.*;
import com.xasecure.view.*;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.xasecure.biz.*;
import com.xasecure.entity.*;
import com.xasecure.service.*;
import com.xasecure.view.*;

@Service
@Scope("singleton")
public class XCredentialStoreService extends XCredentialStoreServiceBase<XXCredentialStore, VXCredentialStore> {

	@Override
	protected void validateForCreate(VXCredentialStore vObj) {
		// TODO Auto-generated method stub

	}

	@Override
	protected void validateForUpdate(VXCredentialStore vObj, XXCredentialStore mObj) {
		// TODO Auto-generated method stub

	}

}
