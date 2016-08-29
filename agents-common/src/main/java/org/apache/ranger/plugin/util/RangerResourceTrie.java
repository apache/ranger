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
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyresourcematcher.RangerPolicyResourceEvaluator;
import org.apache.ranger.plugin.resourcematcher.RangerAbstractResourceMatcher;
import org.apache.ranger.plugin.resourcematcher.RangerResourceMatcher;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class RangerResourceTrie<T extends RangerPolicyResourceEvaluator> {
    private static final Log LOG = LogFactory.getLog(RangerResourceTrie.class);

    private static final String DEFAULT_WILDCARD_CHARS = "*?";

    private final String   resourceName;
    private final boolean  optIgnoreCase;
    private final boolean  optWildcard;
    private final String   wildcardChars;
    private final TrieNode root;

    public RangerResourceTrie(RangerServiceDef.RangerResourceDef resourceDef, List<T> evaluators) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerResourceTrie(" + resourceDef.getName() + ", evaluatorCount=" + evaluators.size() + ")");
        }

        Map<String, String> matcherOptions = resourceDef.getMatcherOptions();
        String              strIgnoreCase  = matcherOptions != null ? matcherOptions.get(RangerAbstractResourceMatcher.OPTION_IGNORE_CASE) : null;
        String              strWildcard    = matcherOptions != null ? matcherOptions.get(RangerAbstractResourceMatcher.OPTION_WILD_CARD) : null;

        this.resourceName  = resourceDef.getName();
        this.optIgnoreCase = strIgnoreCase != null ? Boolean.parseBoolean(strIgnoreCase) : false;
        this.optWildcard   = strWildcard != null ? Boolean.parseBoolean(strWildcard) : false;
        this.wildcardChars = optWildcard ? DEFAULT_WILDCARD_CHARS : "";
        this.root          = new TrieNode(Character.valueOf((char)0));

        for(T evaluator : evaluators) {
            Map<String, RangerPolicyResource> policyResources = evaluator.getPolicyResource();
            RangerPolicyResource              policyResource  = policyResources != null ? policyResources.get(resourceName) : null;

            if(policyResource == null) {
                if(evaluator.getLeafResourceLevel() != null && resourceDef.getLevel() != null && evaluator.getLeafResourceLevel() < resourceDef.getLevel()) {
                    root.addWildcardEvaluator(evaluator);
                }

                continue;
            }

            if(policyResource.getIsExcludes()) {
                root.addWildcardEvaluator(evaluator);
            } else {
                RangerResourceMatcher resourceMatcher = evaluator.getResourceMatcher(resourceName);

                if(resourceMatcher != null && resourceMatcher.isMatchAny()) {
                    root.addWildcardEvaluator(evaluator);
                } else {
                    if(CollectionUtils.isNotEmpty(policyResource.getValues())) {
                        for (String resource : policyResource.getValues()) {
                            insert(resource, policyResource.getIsRecursive(), evaluator);
                        }
                    }
                }
            }
        }

        root.postSetup(null);

        LOG.info(toString());

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerResourceTrie(" + resourceDef.getName() + ", evaluatorCount=" + evaluators.size() + "): " + toString());
        }
    }

    public String getResourceName() {
        return resourceName;
    }

    public List<T> getEvaluatorsForResource(String resource) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerResourceTrie.getEvaluatorsForResource(" + resource + ")");
        }

        List<T> ret = null;

        TrieNode curr = root;

        final int len = resource.length();
        for(int i = 0; i < len; i++) {
            Character ch    = getLookupChar(resource.charAt(i));
            TrieNode  child = curr.getChild(ch);

            if(child == null) {
                ret = curr.getWildcardEvaluators();
                curr = null; // so that curr.getEvaluators() will not be called below
                break;
            }

            curr = child;
        }

        if(ret == null) {
            if(curr != null) {
                ret = curr.getEvaluators();
            }
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerResourceTrie.getEvaluatorsForResource(" + resource + "): evaluatorCount=" + (ret == null ? 0 : ret.size()));
        }

        return ret;
    }

    public TrieData getTrieData() {
        TrieData ret = new TrieData();

        root.populateTrieData(ret);
        ret.maxDepth = getMaxDepth();

        return ret;
    }

    public int getMaxDepth() {
        return root.getMaxDepth();
    }

    public void reorderEvaluators() {
        root.reorderEvaluators(null);
    }

    private final Character getLookupChar(char ch) {
        if(optIgnoreCase) {
            ch = Character.toLowerCase(ch);
        }

        return Character.valueOf(ch);
    }

    private void insert(String resource, boolean isRecursive, T evaluator) {
        TrieNode curr       = root;
        boolean  isWildcard = false;

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
            curr.addWildcardEvaluator(evaluator);
        } else {
            curr.addEvaluator(evaluator);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        TrieData trieData = getTrieData();

        sb.append("resourceName=").append(resourceName);
        sb.append("; optIgnoreCase=").append(optIgnoreCase);
        sb.append("; optWildcard=").append(optWildcard);
        sb.append("; wildcardChars=").append(wildcardChars);
        sb.append("; nodeCount=").append(trieData.nodeCount);
        sb.append("; leafNodeCount=").append(trieData.leafNodeCount);
        sb.append("; singleChildNodeCount=").append(trieData.singleChildNodeCount);
        sb.append("; maxDepth=").append(trieData.maxDepth);
        sb.append("; evaluatorListCount=").append(trieData.evaluatorListCount);
        sb.append("; wildcardEvaluatorListCount=").append(trieData.wildcardEvaluatorListCount);
        sb.append("; evaluatorListRefCount=").append(trieData.evaluatorListRefCount);
        sb.append("; wildcardEvaluatorListRefCount=").append(trieData.wildcardEvaluatorListRefCount);

        return sb.toString();
    }

    public class TrieData {
        int nodeCount                     = 0;
        int leafNodeCount                 = 0;
        int singleChildNodeCount          = 0;
        int maxDepth                      = 0;
        int evaluatorListCount            = 0;
        int wildcardEvaluatorListCount    = 0;
        int evaluatorListRefCount         = 0;
        int wildcardEvaluatorListRefCount = 0;
    }
}

class TrieNode<T extends RangerPolicyResourceEvaluator> {
    private final Character          c;
    private Map<Character, TrieNode> children           = null;
    private List<T>                  evaluators         = null;
    private List<T>                  wildcardEvaluators = null;
    private boolean   isSharingParentWildcardEvaluators = false;

    TrieNode(Character c) {
        this.c = c;
    }

    Character getChar() {
        return c;
    }

    Map<Character, TrieNode> getChildren() {
        return children;
    }

    List<T> getEvaluators() {
        return evaluators;
    }

    List<T> getWildcardEvaluators() {
        return wildcardEvaluators;
    }

    TrieNode getChild(Character c) {
        TrieNode ret = children == null ? null : children.get(c);

        return ret;
    }

    void populateTrieData(RangerResourceTrie.TrieData trieData) {
        trieData.nodeCount++;

        if(wildcardEvaluators != null) {
            if(isSharingParentWildcardEvaluators) {
                trieData.wildcardEvaluatorListRefCount++;
            } else {
                trieData.wildcardEvaluatorListCount++;
            }
        }

        if(evaluators != null) {
            if(evaluators == wildcardEvaluators) {
                trieData.evaluatorListRefCount++;
            } else {
                trieData.evaluatorListCount++;
            }
        }

        if(children != null && children.size() > 0) {
            if(children.size() == 1) {
                trieData.singleChildNodeCount++;
            }

            for(Map.Entry<Character, TrieNode> entry : children.entrySet()) {
                TrieNode child = entry.getValue();

                child.populateTrieData(trieData);
            }
        } else {
            trieData.leafNodeCount++;
        }
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

    void addEvaluator(T evaluator) {
        if(evaluators == null) {
            evaluators = new ArrayList<T>();
        }

        if(!evaluators.contains(evaluator)) {
            evaluators.add(evaluator);
        }
    }

    void addWildcardEvaluator(T evaluator) {
        if(wildcardEvaluators == null) {
            wildcardEvaluators = new ArrayList<T>();
        }

        if(!wildcardEvaluators.contains(evaluator)) {
            wildcardEvaluators.add(evaluator);
        }
    }

    void postSetup(List<T> parentWildcardEvaluators) {
        // finalize wildcard-evaluators list by including parent's wildcard evaluators
        if(parentWildcardEvaluators != null) {
            if(CollectionUtils.isEmpty(this.wildcardEvaluators)) {
                this.wildcardEvaluators = parentWildcardEvaluators;
            } else {
                for (T evaluator : parentWildcardEvaluators) {
                    addWildcardEvaluator(evaluator);
                }
            }
        }
        this.isSharingParentWildcardEvaluators = wildcardEvaluators == parentWildcardEvaluators;

        // finalize evaluators list by including wildcard evaluators
        if(wildcardEvaluators != null) {
            if(CollectionUtils.isEmpty(this.evaluators)) {
                this.evaluators = wildcardEvaluators;
            } else {
                for (T evaluator : wildcardEvaluators) {
                    addEvaluator(evaluator);
                }
            }
        }

        if(!isSharingParentWildcardEvaluators && CollectionUtils.isNotEmpty(wildcardEvaluators)) {
            Collections.sort(wildcardEvaluators);
        }

        if(evaluators != wildcardEvaluators && CollectionUtils.isNotEmpty(evaluators)) {
            Collections.sort(evaluators);
        }

        if(children != null) {
            for(Map.Entry<Character, TrieNode> entry : children.entrySet()) {
                TrieNode child = entry.getValue();

                child.postSetup(wildcardEvaluators);
            }
        }
    }

    void reorderEvaluators(List<T> parentWildcardEvaluators) {
        boolean isEvaluatorsSameAsWildcardEvaluators = evaluators == wildcardEvaluators;

        if(isSharingParentWildcardEvaluators) {
            wildcardEvaluators = parentWildcardEvaluators;
        } else {
            wildcardEvaluators = getSortedCopy(wildcardEvaluators);
        }

        if(isEvaluatorsSameAsWildcardEvaluators) {
            evaluators = wildcardEvaluators;
        } else {
            evaluators = getSortedCopy(evaluators);
        }

        if(children != null) {
            for(Map.Entry<Character, TrieNode> entry : children.entrySet()) {
                TrieNode child = entry.getValue();

                child.reorderEvaluators(wildcardEvaluators);
            }
        }
    }

    public void toString(String prefix, StringBuilder sb) {
        String nodeValue = prefix;

        if(c != 0) {
            nodeValue += c;
        }

        sb.append("nodeValue=").append(nodeValue);
        sb.append("; childCount=").append(children == null ? 0 : children.size());
        sb.append("; evaluators=[ ");
        if(evaluators != null) {
            for(T evaluator : evaluators) {
                sb.append(evaluator.getId()).append(" ");
            }
        }
        sb.append("]");

        sb.append("; wildcardEvaluators=[ ");
        if(wildcardEvaluators != null) {
            for(T evaluator : wildcardEvaluators) {
                sb.append(evaluator.getId()).append(" ");
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
        children           = null;
        evaluators         = null;
        wildcardEvaluators = null;
    }

    private List<T> getSortedCopy(List<T> evaluators) {
        final List<T> ret;

        if(CollectionUtils.isNotEmpty(evaluators)) {
            ret = new ArrayList<T>(evaluators);

            Collections.sort(ret);
        } else {
            ret = evaluators;
        }

        return ret;
    }
}
