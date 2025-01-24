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

/**
 *
 */
package org.apache.ranger.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SearchCriteria {
    Logger logger = LoggerFactory.getLogger(SearchCriteria.class);

    int                     startIndex;
    int                     maxRows          = Integer.MAX_VALUE;
    String                  sortBy;
    String                  sortType;
    boolean                 getCount         = true;
    Number                  ownerId;
    boolean                 familyOnly;
    boolean                 getChildren;
    boolean                 isDistinct;
    HashMap<String, Object> paramList        = new HashMap<>();
    Set<String>             nullParamList    = new HashSet<>();
    Set<String>             notNullParamList = new HashSet<>();
    List<SearchGroup>       searchGroups     = new ArrayList<>();

    public SearchCriteria() {}

    public SearchCriteria(String paramName, Object paramValue) {
        addParam(paramName, paramValue);
    }

    /**
     * @return the startIndex
     */
    public int getStartIndex() {
        return startIndex;
    }

    /**
     * @param startIndex the startIndex to set
     */
    public void setStartIndex(int startIndex) {
        this.startIndex = startIndex;
    }

    /**
     * @return the maxRows
     */
    public int getMaxRows() {
        return maxRows;
    }

    /**
     * @param maxRows the maxRows to set
     */
    public void setMaxRows(int maxRows) {
        this.maxRows = maxRows;
    }

    /**
     * @return the sortBy
     */
    public String getSortBy() {
        return sortBy;
    }

    /**
     * @param sortBy the sortBy to set
     */
    public void setSortBy(String sortBy) {
        this.sortBy = sortBy;
    }

    /**
     * @return the sortType
     */
    public String getSortType() {
        return sortType;
    }

    /**
     * @param sortType the sortType to set
     */
    public void setSortType(String sortType) {
        this.sortType = sortType;
    }

    public boolean isGetCount() {
        return getCount;
    }

    public void setGetCount(boolean getCount) {
        this.getCount = getCount;
    }

    public Number getOwnerId() {
        return ownerId;
    }

    public void setOwnerId(Number ownerId) {
        this.ownerId = ownerId;
    }

    public boolean isGetChildren() {
        return getChildren;
    }

    public void setGetChildren(boolean getChildren) {
        this.getChildren = getChildren;
    }

    /**
     * @return the paramList
     */
    public HashMap<String, Object> getParamList() {
        return paramList;
    }

    /**
     * @param name
     * @param value
     */
    public void addParam(String name, Object value) {
        paramList.put(name, value);
    }

    public Object getParamValue(String name) {
        return paramList.get(name);
    }

    /**
     * @return the nullParamList
     */
    public Set<String> getNullParamList() {
        return nullParamList;
    }

    /**
     * @return the notNullParamList
     */
    public Set<String> getNotNullParamList() {
        return notNullParamList;
    }

    /**
     * @return the searchGroups
     */
    public List<SearchGroup> getSearchGroups() {
        return searchGroups;
    }

    /**
     * @return the isDistinct
     */
    public boolean isDistinct() {
        return isDistinct;
    }

    /**
     * @param isDistinct the isDistinct to set
     */
    public void setDistinct(boolean isDistinct) {
//        int dbFlavor = RangerBizUtil.getDBFlavor();
//        if (isDistinct && dbFlavor == AppConstants.DB_FLAVOR_ORACLE) {
//            isDistinct = false;
//            logger.debug("Database flavor is `ORACLE` so ignoring DISTINCT clause from select statement.");
//        }

        this.isDistinct = isDistinct;
    }
}
