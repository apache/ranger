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

package org.apache.ranger.model;

import org.apache.ranger.plugin.model.RangerTagDef;
import org.apache.ranger.plugin.model.RangerTagResourceMap;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by akulkarni on 9/10/15.
 */
public interface TagSource {

	boolean initialize(Properties properties);

	void setTagSink(TagSink sink);

	void updateSink() throws Exception;

	void start();

	boolean isChanged();

	List<RangerTagDef> fetchAllTagDefs(String syncSentinel) throws Exception;

	List<RangerTagDef> receiveUpdatesToTagDefs() throws Exception;

	List<RangerTagResourceMap> fetchAllTaggedEntities() throws Exception;

	List<RangerTagResourceMap> receiveUpdatesToTaggedEntities() throws Exception;

}
