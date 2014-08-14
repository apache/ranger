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
public class XGroupGroupService extends XGroupGroupServiceBase<XXGroupGroup, VXGroupGroup> {

	@Override
	protected void validateForCreate(VXGroupGroup vObj) {
		// TODO Auto-generated method stub

	}

	@Override
	protected void validateForUpdate(VXGroupGroup vObj, XXGroupGroup mObj) {
		// TODO Auto-generated method stub

	}

}
