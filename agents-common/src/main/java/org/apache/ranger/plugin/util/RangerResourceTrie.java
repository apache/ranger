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


import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyevaluator.RangerPolicyEvaluator;
import org.apache.ranger.plugin.resourcematcher.RangerAbstractResourceMatcher;
import org.apache.ranger.plugin.resourcematcher.RangerResourceMatcher;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class RangerResourceTrie {
    private static final Log LOG = LogFactory.getLog(RangerResourceTrie.class);

    private static final String DEFAULT_WILDCARD_CHARS = "*?";

    private final String   resourceName;
    private final boolean  optIgnoreCase;
    private final boolean  optWildcard;
    private final String   wildcardChars;
    private final TrieNode root;

    public RangerResourceTrie(RangerServiceDef.RangerResourceDef resourceDef, List<RangerPolicyEvaluator> evaluators) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerResourceTrie(" + resourceDef.getName() + ", evaluatorCount=" + evaluators.size() + ")");
        }

        Map<String, String> matcherOptions = resourceDef.getMatcherOptions();
        String              strIgnoreCase  = matcherOptions != null ? matcherOptions.get(RangerAbstractResourceMatcher.OPTION_IGNORE_CASE) : null;
        String              strWildcard    = matcherOptions != null ? matcherOptions.get(RangerAbstractResourceMatcher.OPTION_WILD_CARD) : null;

        this.resourceName  = resourceDef.getName();
        this.optIgnoreCase = strIgnoreCase != null ? Boolean.parseBoolean(strIgnoreCase) : false;
        this.optWildcard   = strWildcard != null ? Boolean.parseBoolean(strWildcard) : false;;
        this.wildcardChars = optWildcard ? DEFAULT_WILDCARD_CHARS : "";
        this.root          = new TrieNode(Character.valueOf((char)0));

        for(RangerPolicyEvaluator evaluator : evaluators) {
            RangerPolicy                      policy          = evaluator.getPolicy();
            Map<String, RangerPolicyResource> policyResources = policy != null ? policy.getResources() : null;
            RangerPolicyResource              policyResource  = policyResources != null ? policyResources.get(resourceName) : null;

            if(policyResource == null) {
                continue;
            }

            if(policyResource.getIsExcludes()) {
                root.addWildcardPolicy(evaluator);
            } else {
                RangerResourceMatcher resourceMatcher = evaluator.getResourceMatcher(resourceName);

                if(resourceMatcher != null && resourceMatcher.isMatchAny()) {
                    root.addWildcardPolicy(evaluator);
                } else {
                    for (String resource : policyResource.getValues()) {
                        insert(resource, policyResource.getIsRecursive(), evaluator);
                    }
                }
            }
        }

        root.postSetup();

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerResourceTrie(" + resourceDef.getName() + ", evaluatorCount=" + evaluators.size() + "): " + toString());
        }
    }

    public String getResourceName() {
        return resourceName;
    }

    public List<RangerPolicyEvaluator> getPoliciesForResource(String resource) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerResourceTrie.getPoliciesForResource(" + resource + ")");
        }

        List<RangerPolicyEvaluator> ret = null;

        TrieNode curr = root;

        final int len = resource.length();
        for(int i = 0; i < len; i++) {
            Character ch    = getLookupChar(resource.charAt(i));
            TrieNode  child = curr.getChild(ch);

            if(child == null) {
                ret = curr.getWildcardPolicies();
                curr = null; // so that curr.getPolicies() will not be called below
                break;
            }

            curr = child;
        }

        if(ret == null) {
            if(curr != null) {
                ret = curr.getPolicies();
            }
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerResourceTrie.getPoliciesForResource(" + resource + "): evaluatorCount=" + (ret == null ? 0 : ret.size()));
        }

        return ret;
    }

    public int getNodeCount() {
        return root.getNodeCount();
    }

    public int getMaxDepth() {
        return root.getMaxDepth();
    }

    public void reorderPolicyEvaluators() {
        root.reorderPolicyEvaluators();
    }

    private Character getLookupChar(char ch) {
        return optIgnoreCase ? Character.valueOf(Character.toLowerCase(ch)) : Character.valueOf(ch);
    }

    private void insert(String resource, boolean isRecursive, RangerPolicyEvaluator evaluator) {
        TrieNode curr       = root;
        boolean  isWildcard = false;

        if(optIgnoreCase) {
            resource = resource.toLowerCase();
        }

        final int len = resource.length();
        for(int i = 0; i < len; i++) {
            Character ch = getLookupChar(resource.charAt(i));

            if(optWildcard) {
                if (wildcardChars.indexOf(ch) != -1) {
                    isWildcard = true;
                    break;
                }
            }

            curr = curr.getOrCreateChild(ch);
        }

        if(isWildcard || isRecursive) {
            curr.addWildcardPolicy(evaluator);
        } else {
            curr.addPolicy(evaluator);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("nodeCount=").append(getNodeCount());
        sb.append("; maxDepth=").append(getMaxDepth());
        sb.append(Character.LINE_SEPARATOR);
        root.toString("", sb);

        return sb.toString();
    }
}

class TrieNode {
    private final Character             c;
    private Map<Character, TrieNode>    children         = null;
    private List<RangerPolicyEvaluator> policies         = null;
    private List<RangerPolicyEvaluator> wildcardPolicies = null;

    TrieNode(Character c) {
        this.c = c;
    }

    Character getChar() {
        return c;
    }

    Map<Character, TrieNode> getChildren() {
        return children;
    }

    List<RangerPolicyEvaluator> getPolicies() {
        return policies;
    }

    List<RangerPolicyEvaluator> getWildcardPolicies() {
        return wildcardPolicies;
    }

    TrieNode getChild(Character c) {
        TrieNode ret = children == null ? null : children.get(c);

        return ret;
    }

    int getNodeCount() {
        int ret = 1;

        if(children != null) {
            for(Map.Entry<Character, TrieNode> entry : children.entrySet()) {
                TrieNode child = entry.getValue();

                ret += child.getNodeCount();
            }
        }

        return ret;
    }

    int getMaxDepth() {
        int ret = 0;

        if(children != null) {
            for(Map.Entry<Character, TrieNode> entry : children.entrySet()) {
                TrieNode child = entry.getValue();

                int maxChildDepth = child.getMaxDepth();

                if(maxChildDepth > ret) {
                    ret = maxChildDepth;
                }
            }
        }

        return ret + 1;
    }

    TrieNode getOrCreateChild(Character c) {
        if(children == null) {
            children = new HashMap<Character, TrieNode>();
        }

        TrieNode child = children.get(c);

        if(child == null) {
            child = new TrieNode(c);
            children.put(c, child);
        }

        return child;
    }

    void addPolicy(RangerPolicyEvaluator evaluator) {
        if(policies == null) {
            policies = new ArrayList<RangerPolicyEvaluator>();
        }

        if(!policies.contains(evaluator)) {
            policies.add(evaluator);
        }
    }

    void addPolicies(List<RangerPolicyEvaluator> evaluators) {
        if(CollectionUtils.isNotEmpty(evaluators)) {
            for(RangerPolicyEvaluator evaluator : evaluators) {
                addPolicy(evaluator);
            }
        }
    }

    void addWildcardPolicy(RangerPolicyEvaluator evaluator) {
        if(wildcardPolicies == null) {
            wildcardPolicies = new ArrayList<RangerPolicyEvaluator>();
        }

        if(!wildcardPolicies.contains(evaluator)) {
            wildcardPolicies.add(evaluator);
        }
    }

    void addWildcardPolicies(List<RangerPolicyEvaluator> evaluators) {
        if(CollectionUtils.isNotEmpty(evaluators)) {
            for(RangerPolicyEvaluator evaluator : evaluators) {
                addWildcardPolicy(evaluator);
            }
        }
    }

    void postSetup() {
        addPolicies(wildcardPolicies);

        if(wildcardPolicies != null) {
            Collections.sort(wildcardPolicies);
        }

        if(policies != null) {
            Collections.sort(policies);
        }

        if(children != null) {
            for(Map.Entry<Character, TrieNode> entry : children.entrySet()) {
                TrieNode child = entry.getValue();

                child.addWildcardPolicies(wildcardPolicies);

                child.postSetup();
            }
        }
    }

    void reorderPolicyEvaluators() {
        wildcardPolicies = getSortedCopy(wildcardPolicies);
        policies         = getSortedCopy(policies);
    }

    public void toString(String prefix, StringBuilder sb) {
        String nodeValue = prefix;

        if(c != 0) {
            nodeValue += c;
        }

        sb.append("nodeValue=").append(nodeValue);
        sb.append("; childCount=").append(children == null ? 0 : children.size());
        sb.append("; policies=[ ");
        if(policies != null) {
            for(RangerPolicyEvaluator evaluator : policies) {
                sb.append(evaluator.getPolicy().getId()).append(" ");
            }
        }
        sb.append("]");

        sb.append("; wildcardPolicies=[ ");
        if(wildcardPolicies != null) {
            for(RangerPolicyEvaluator evaluator : wildcardPolicies) {
                sb.append(evaluator.getPolicy().getId()).append(" ");
            }
        }
        sb.append("]");
        sb.append(Character.LINE_SEPARATOR);

        if(children != null) {
            for(Map.Entry<Character, TrieNode> entry : children.entrySet()) {
                TrieNode child = entry.getValue();

                child.toString(nodeValue, sb);
            }
        }
    }

    public void clear() {
        children         = null;
        policies         = null;
        wildcardPolicies = null;
    }

    private List<RangerPolicyEvaluator> getSortedCopy(List<RangerPolicyEvaluator> evaluators) {
        final List<RangerPolicyEvaluator> ret;

        if(CollectionUtils.isNotEmpty(evaluators)) {
            ret = new ArrayList<RangerPolicyEvaluator>(wildcardPolicies);

            Collections.sort(ret);
        } else {
            ret = evaluators;
        }

        return ret;
    }
}
