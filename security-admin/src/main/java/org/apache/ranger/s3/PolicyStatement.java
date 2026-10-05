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
package org.apache.ranger.s3;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class PolicyStatement {
    @JsonProperty("Effect")
    private String effect;
    @JsonProperty("Principal")
    private Map<String, List<String>> principal;
    @JsonProperty("Action")
    private List<String> action;
    @JsonProperty("Resource")
    private String resource;

    // Getters and setters
    public String getEffect() {
        return effect;
    }

    public void setEffect(String effect) {
        this.effect = effect;
    }

    public Map<String, List<String>> getPrincipal() {
        return principal;
    }

    public void setPrincipal(Map<String, List<String>> principal) {
        this.principal = principal;
    }

    public List<String> getAction() {
        return action;
    }

    @JsonProperty("Action")
    public void setAction(Object action) {
        if (action == null) {
            this.action = new ArrayList<>();
        } else if (action instanceof String) {
            // Handle single action as string
            this.action = new ArrayList<>();
            this.action.add((String) action);
        } else if (action instanceof List) {
            // Handle multiple actions as list
            this.action = (List<String>) action;
        } else {
            // Fallback
            this.action = new ArrayList<>();
        }
    }

    public void setAction(List<String> action) {
        this.action = action;
    }

    public String getResource() {
        return resource;
    }

    public void setResource(String resource) {
        this.resource = resource;
    }
}
