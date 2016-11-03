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

package org.apache.ranger.plugin.contextenricher;

import org.apache.ranger.plugin.model.RangerTag;
import org.apache.ranger.plugin.policyresourcematcher.RangerPolicyResourceMatcher;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.Map;

@JsonAutoDetect(fieldVisibility=JsonAutoDetect.Visibility.ANY)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)

// This class needs above annotations for policy-engine unit tests involving RangerTagForEval objects that are initialized
// from JSON specification

public class RangerTagForEval {
    private RangerTag tag;
    private RangerPolicyResourceMatcher.MatchType matchType = RangerPolicyResourceMatcher.MatchType.SELF;

    private RangerTagForEval() {
    }

    public RangerTagForEval(RangerTag tag, RangerPolicyResourceMatcher.MatchType matchType) {
        this.tag = tag;
        this.matchType = matchType;
    }

    public void setTag(RangerTag tag) {
        this.tag = tag;
    }
    public void setMatchType(RangerPolicyResourceMatcher.MatchType matchType) {
        this.matchType = matchType;
    }

    public RangerPolicyResourceMatcher.MatchType getMatchType() {
        return matchType;
    }

    public String getType() { return tag.getType();}
    public Map<String, String> getAttributes() {
        return tag.getAttributes();
    }
    public Long getVersion() {
        return tag.getVersion();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        toString(sb);

        return sb.toString();
    }

    public StringBuilder toString(StringBuilder sb) {
        sb.append("RangerTagForEval={");
        sb.append("tag={");
        if (tag != null) {
            tag.toString(sb);
        }
        sb.append("}");
        sb.append(", {matchType={").append(matchType).append("}");
        sb.append("}");
        return sb;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((tag == null) ? 0 : tag.hashCode());
        result = prime * result
                + ((matchType == null) ? 0 : matchType.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        RangerTagForEval other = (RangerTagForEval) obj;
        if (tag == null) {
            if (other.tag != null)
                return false;
        } else if (!tag.equals(other.tag))
            return false;
        if (matchType == null) {
            if (other.matchType != null)
                return false;
        } else if (!matchType.equals(other.matchType))
            return false;

        return true;
    }
}
