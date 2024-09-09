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

package org.apache.ranger.plugin.util;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;


@JsonAutoDetect(getterVisibility= JsonAutoDetect.Visibility.NONE, setterVisibility= JsonAutoDetect.Visibility.NONE, fieldVisibility= JsonAutoDetect.Visibility.ANY)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown=true)
public class RangerPurgeResult {
    private String recordType;
    private Long   totalRecordCount;
    private Long   purgedRecordCount;

    public RangerPurgeResult() { }

    public RangerPurgeResult(String recordType, Long totalRecordCount, Long purgedRecordCount) {
        this.recordType        = recordType;
        this.totalRecordCount  = totalRecordCount;
        this.purgedRecordCount = purgedRecordCount;
    }

    public String getRecordType() {
        return recordType;
    }

    public void setRecordType(String recordType) {
        this.recordType = recordType;
    }

    public Long getTotalRecordCount() {
        return totalRecordCount;
    }

    public void setTotalRecordCount(Long totalRecordCount) {
        this.totalRecordCount = totalRecordCount;
    }

    public Long getPurgedRecordCount() {
        return purgedRecordCount;
    }

    public void setPurgedRecordCount(Long purgedRecordCount) {
        this.purgedRecordCount = purgedRecordCount;
    }

    @Override
    public String toString( ) {
        StringBuilder sb = new StringBuilder();

        toString(sb);

        return sb.toString();
    }

    public StringBuilder toString(StringBuilder sb) {
        sb.append("RangerPurgeResult={");

        sb.append("recordType={").append(recordType).append("} ");
        sb.append("totalRecordCount={").append(totalRecordCount).append("} ");
        sb.append("purgedRecordCount={").append(purgedRecordCount).append("} ");

        sb.append("}");

        return sb;
    }

}
