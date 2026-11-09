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
package org.apache.ranger.plugin.policyengine;

import org.apache.ranger.plugin.policyresourcematcher.RangerPolicyResourceMatcher;
import org.apache.ranger.plugin.policyresourcematcher.RangerPolicyResourceMatcher.MatchType;

import java.io.Serializable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class RangerConstraintResult {
    public static final Comparator<RangerConstraintResult> CONSTRAINT_RESULT_COMPARATOR = new ConstraintResultComparator();

    private final Map<String, Object> parameters;
    private String                    constraintName;
    private long                      policyId;
    private Long                      policyVersion;
    private int                       policyPriority;
    private MatchType                 matchType;

    public RangerConstraintResult(String constraintName, Map<String, Object> parameters, long policyId, Long policyVersion, int policyPriority, MatchType matchType) {
        this.constraintName = constraintName;
        this.parameters     = parameters == null ? new HashMap<>() : new HashMap<>(parameters);
        this.policyId       = policyId;
        this.policyVersion  = policyVersion;
        this.policyPriority = policyPriority;
        this.matchType      = matchType;
    }

    public String getConstraintName() {
        return constraintName;
    }

    public Map<String, Object> getParameters() {
        return parameters;
    }

    public long getPolicyId() {
        return policyId;
    }

    public Long getPolicyVersion() {
        return policyVersion;
    }

    public int getPolicyPriority() {
        return policyPriority;
    }

    public MatchType getMatchType() {
        return matchType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(constraintName, parameters, policyId, policyVersion, policyPriority, matchType);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        RangerConstraintResult other = (RangerConstraintResult) obj;

        return policyId == other.policyId &&
                policyPriority == other.policyPriority &&
                Objects.equals(policyVersion, other.policyVersion) &&
                Objects.equals(constraintName, other.constraintName) &&
                Objects.equals(matchType, other.matchType) &&
                Objects.equals(parameters, other.parameters);
    }

    @Override
    public String toString() {
        return "RangerConstraintResult{constraintName=" + constraintName +
                ", parameters=" + parameters +
                ", policyId=" + policyId +
                ", policyVersion=" + policyVersion +
                ", policyPriority=" + policyPriority +
                ", matchType=" + matchType +
                "}";
    }

    /**
     * Orders by constraint name, policy priority (higher first), match type, then policy id.
     */
    public static class ConstraintResultComparator implements Comparator<RangerConstraintResult>, Serializable {
        @Override
        public int compare(RangerConstraintResult me, RangerConstraintResult other) {
            int ret = compareStrings(me.getConstraintName(), other.getConstraintName());

            if (ret == 0) {
                ret = Integer.compare(other.getPolicyPriority(), me.getPolicyPriority());
            }

            if (ret == 0) {
                ret = RangerPolicyResourceMatcher.MATCH_TYPE_COMPARATOR.compare(me.getMatchType(), other.getMatchType());
            }

            if (ret == 0) {
                ret = Long.compare(me.getPolicyId(), other.getPolicyId());
            }

            return ret;
        }

        private static int compareStrings(String me, String other) {
            return me == null ? (other == null ? 0 : -1) : (other == null ? 1 : me.compareTo(other));
        }
    }
}
