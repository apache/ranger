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

import com.xasecure.entity.XXAccessAudit;
import com.xasecure.common.*;
import com.xasecure.common.db.*;
import com.xasecure.entity.*;

public class XXAccessAuditDao extends BaseDao<XXAccessAudit> {

    public XXAccessAuditDao( XADaoManagerBase daoManager ) {
		super(daoManager, "loggingPU");
    }
}

