/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.ranger.authorization.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.thirdparty.com.google.common.base.MoreObjects;
import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class RangerAuthorizationFilter extends FilterBase {
    private static final Logger LOG = LoggerFactory.getLogger(RangerAuthorizationFilter.class.getName());

    final Set<String>              familiesAccessAllowed;
    final Set<String>              familiesAccessDenied;
    final Set<String>              familiesAccessIndeterminate;
    final Map<String, Set<String>> columnsAccessAllowed;
    final Set<String>              familiesFullyAuthorized;
    final AuthorizationSession     session;
    final HbaseAuditHandler        auditHandler = HbaseFactory.getInstance().getAuditHandler();

    public RangerAuthorizationFilter(AuthorizationSession session, Set<String> familiesAccessAllowed, Set<String> familiesAccessDenied, Set<String> familiesAccessIndeterminate, Map<String, Set<String>> columnsAccessAllowed, Set<String> familiesFullyAuthorized) {
        // the class assumes that all of these can be empty but none of these can be null
        this.familiesAccessAllowed       = familiesAccessAllowed;
        this.familiesAccessDenied        = familiesAccessDenied;
        this.familiesAccessIndeterminate = familiesAccessIndeterminate;
        this.columnsAccessAllowed        = columnsAccessAllowed;
        this.familiesFullyAuthorized     = familiesFullyAuthorized;

        // this session should have everything set on it except family and column which would be altered based on need
        this.session = session;

        // we don't want to audit denial, so we need to make sure the hander is what we need it to be.
        this.session.auditHandler(auditHandler);
    }

    @Override
    public ReturnCode filterKeyValue(Cell kv) {
        LOG.debug("==> filterKeyValue");

        String family      = null;
        byte[] familyBytes = CellUtil.cloneFamily(kv);

        if (familyBytes.length > 0) {
            family = Bytes.toString(familyBytes);

            LOG.debug("filterKeyValue: evaluating family[{}].", family);
        }

        String column    = null;
        byte[] qualifier = CellUtil.cloneQualifier(kv);

        if (qualifier.length > 0) {
            column = Bytes.toString(qualifier);

            LOG.debug("filterKeyValue: evaluating column[{}].", column);
        } else {
            LOG.warn("filterKeyValue: empty/null column set! Unexpected!");
        }

        ReturnCode result          = ReturnCode.NEXT_COL;
        boolean    authCheckNeeded = false;

        if (family == null) {
            LOG.warn("filterKeyValue: Unexpected - null/empty family! Access denied!");
        } else if (familiesAccessDenied.contains(family)) {
            LOG.debug("filterKeyValue: family found in access denied families cache.  Access denied.");
        } else if (session.getPropertyIsColumnAuthOptimizationEnabled() && familiesFullyAuthorized.contains(family)) {
            LOG.debug("filterKeyValue: ColumnAuthOptimizationEnabled and family found in fully authorized families cache.  Column authorization is not required");

            result = ReturnCode.INCLUDE;
        } else if (columnsAccessAllowed.containsKey(family)) {
            LOG.debug("filterKeyValue: family found in column level access results cache.");

            if (columnsAccessAllowed.get(family).contains(column)) {
                LOG.debug("filterKeyValue: family/column found in column level access results cache. Access allowed.");

                result = ReturnCode.INCLUDE;
            } else {
                LOG.debug("filterKeyValue: family/column not in column level access results cache. Access denied.");
            }
        } else if (familiesAccessAllowed.contains(family)) {
            LOG.debug("filterKeyValue: family found in access allowed families cache.  Must re-authorize for correct audit generation.");

            authCheckNeeded = true;
        } else if (familiesAccessIndeterminate.contains(family)) {
            LOG.debug("filterKeyValue: family found in indeterminate families cache.  Evaluating access...");

            authCheckNeeded = true;
        } else {
            LOG.warn("filterKeyValue: Unexpected - alien family encountered that wasn't seen by pre-hook!  Access Denied.!");
        }

        if (authCheckNeeded) {
            LOG.debug("filterKeyValue: Checking authorization...");

            session.columnFamily(family)
                    .column(column)
                    .buildRequest()
                    .authorize();

            // must always purge the captured audit event out of the audit handler to avoid messing up the next check
            AuthzAuditEvent auditEvent = auditHandler.getAndDiscardMostRecentEvent();

            if (session.isAuthorized()) {
                LOG.debug("filterKeyValue: Access granted.");

                result = ReturnCode.INCLUDE;

                if (auditEvent != null) {
                    LOG.debug("filterKeyValue: access is audited.");

                    auditHandler.logAuthzAudits(Collections.singletonList(auditEvent));
                } else {
                    LOG.debug("filterKeyValue: no audit event returned.  Access not audited.");
                }
            } else {
                LOG.debug("filterKeyValue: Access denied.  Denial not audited.");
            }
        }

        LOG.debug("filterKeyValue: {}", result);

        return result;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(getClass())
                .add("familiesAccessAllowed", familiesAccessAllowed)
                .add("familiesAccessDenied", familiesAccessDenied)
                .add("familiesAccessUnknown", familiesAccessIndeterminate)
                .add("columnsAccessAllowed", columnsAccessAllowed)
                .toString();
    }
}
