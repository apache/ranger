package com.xasecure.db;
/*
 * Copyright (c) 2014 XASecure
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of
 * XASecure. ("Confidential Information").  You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with XASecure.
 */

import com.xasecure.entity.XXCredentialStore;

import com.xasecure.common.*;
import com.xasecure.common.db.*;
import com.xasecure.entity.*;

public class XXCredentialStoreDao extends BaseDao<XXCredentialStore> {

    public XXCredentialStoreDao( XADaoManagerBase daoManager ) {
		super(daoManager);
    }
}

