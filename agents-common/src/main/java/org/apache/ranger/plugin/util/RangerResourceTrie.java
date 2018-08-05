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
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyresourcematcher.RangerPolicyResourceEvaluator;
import org.apache.ranger.plugin.resourcematcher.RangerAbstractResourceMatcher;
import org.apache.ranger.plugin.resourcematcher.RangerResourceMatcher;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RangerResourceTrie<T extends RangerPolicyResourceEvaluator> {
    private static final Log LOG = LogFactory.getLog(RangerResourceTrie.class);
    private static final Log PERF_TRIE_INIT_LOG = RangerPerfTracer.getPerfLogger("resourcetrie.init");
    private static final Log PERF_TRIE_OP_LOG = RangerPerfTracer.getPerfLogger("resourcetrie.op");

    private static final String DEFAULT_WILDCARD_CHARS = "*?";

    private final String        resourceName;
    private final boolean       optIgnoreCase;
    private final boolean       optWildcard;
    private final String        wildcardChars;
    private final TrieNode<T>   root;
    private final Comparator<T> comparator;

    public RangerResourceTrie(RangerServiceDef.RangerResourceDef resourceDef, List<T> evaluators) {
        this(resourceDef, evaluators, null);
    }

    public RangerResourceTrie(RangerServiceDef.RangerResourceDef resourceDef, List<T> evaluators, Comparator<T> comparator) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerResourceTrie(" + resourceDef.getName() + ", evaluatorCount=" + evaluators.size() + ")");
        }

        RangerPerfTracer perf = null;

        if(RangerPerfTracer.isPerfTraceEnabled(PERF_TRIE_INIT_LOG)) {
            perf = RangerPerfTracer.getPerfTracer(PERF_TRIE_INIT_LOG, "RangerResourceTrie(name=" + resourceDef.getName() + ")");
        }

        Map<String, String> matcherOptions = resourceDef.getMatcherOptions();

        boolean optReplaceTokens = RangerAbstractResourceMatcher.getOptionReplaceTokens(matcherOptions);

        String tokenReplaceSpecialChars = "";

        if(optReplaceTokens) {
            char delimiterStart  = RangerAbstractResourceMatcher.getOptionDelimiterStart(matcherOptions);
            char delimiterEnd    = RangerAbstractResourceMatcher.getOptionDelimiterEnd(matcherOptions);
            char delimiterEscape = RangerAbstractResourceMatcher.getOptionDelimiterEscape(matcherOptions);

            tokenReplaceSpecialChars += delimiterStart;
            tokenReplaceSpecialChars += delimiterEnd;
            tokenReplaceSpecialChars += delimiterEscape;
        }

        this.resourceName  = resourceDef.getName();
        this.optIgnoreCase = RangerAbstractResourceMatcher.getOptionIgnoreCase(matcherOptions);
        this.optWildcard   = RangerAbstractResourceMatcher.getOptionWildCard(matcherOptions);
        this.wildcardChars = optWildcard ? DEFAULT_WILDCARD_CHARS + tokenReplaceSpecialChars : "" + tokenReplaceSpecialChars;
        this.root          = new TrieNode<>(null);
        this.comparator    = comparator;

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

                if(resourceMatcher != null && (resourceMatcher.isMatchAny())) {
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

        root.postSetup(null, comparator);

        RangerPerfTracer.logAlways(perf);

        if (PERF_TRIE_INIT_LOG.isDebugEnabled()) {
            PERF_TRIE_INIT_LOG.debug(toString());
        }

        if (PERF_TRIE_INIT_LOG.isTraceEnabled()) {
            StringBuilder sb = new StringBuilder();
            root.toString("", sb);
            PERF_TRIE_INIT_LOG.trace("Trie Dump:\n{" + sb.toString() + "}");
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerResourceTrie(" + resourceDef.getName() + ", evaluatorCount=" + evaluators.size() + "): " + toString());
        }
    }

    public String getResourceName() {
        return resourceName;
    }

    public List<T> getEvaluatorsForResource(Object resource) {
        if (resource instanceof String) {
            return getEvaluatorsForResource((String) resource);
        } else if (resource instanceof Collection) {
            if (CollectionUtils.isEmpty((Collection) resource)) {  // treat empty collection same as empty-string
                return getEvaluatorsForResource("");
            } else {
                @SuppressWarnings("unchecked")
                Collection<String> resources = (Collection<String>) resource;

                return getEvaluatorsForResources(resources);
            }
        }

        return null;
    }

    private TrieData getTrieData() {
        TrieData ret = new TrieData();

        root.populateTrieData(ret);
        ret.maxDepth = getMaxDepth();

        return ret;
    }

    private int getMaxDepth() {
        return root.getMaxDepth();
    }

    private Character getLookupChar(char ch) {
        return optIgnoreCase ? Character.toLowerCase(ch) : ch;
    }

    private Character getLookupChar(String str, int index) {
        return getLookupChar(str.charAt(index));
    }

    private void insert(String resource, boolean isRecursive, T evaluator) {

        RangerPerfTracer perf = null;

        if(RangerPerfTracer.isPerfTraceEnabled(PERF_TRIE_INIT_LOG)) {
            perf = RangerPerfTracer.getPerfTracer(PERF_TRIE_INIT_LOG, "RangerResourceTrie.insert(resource=" + resource + ")");
        }

        TrieNode<T> curr       = root;

        final String prefix       = getNonWildcardPrefix(resource);
        final boolean isWildcard  = prefix.length() != resource.length();

        if (StringUtils.isNotEmpty(prefix)) {
            curr = curr.getOrCreateChild(prefix);
        }

        if(isWildcard || isRecursive) {
            curr.addWildcardEvaluator(evaluator);
        } else {
            curr.addEvaluator(evaluator);
        }

        RangerPerfTracer.logAlways(perf);
    }

    private String getNonWildcardPrefix(String str) {
        if (!optWildcard) return str;
        int minIndex = str.length();
        for (int i = 0; i < wildcardChars.length(); i++) {
            int index = str.indexOf(wildcardChars.charAt(i));
            if (index != -1 && index < minIndex) {
                minIndex = index;
            }
        }
        return str.substring(0, minIndex);
    }

    private List<T> getEvaluatorsForResource(String resource) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerResourceTrie.getEvaluatorsForResource(" + resource + ")");
        }

        RangerPerfTracer perf = null;

        if(RangerPerfTracer.isPerfTraceEnabled(PERF_TRIE_OP_LOG)) {
            perf = RangerPerfTracer.getPerfTracer(PERF_TRIE_OP_LOG, "RangerResourceTrie.getEvaluatorsForResource(resource=" + resource + ")");
        }

        TrieNode<T> curr = root;

        final int   len  = resource.length();
        int         i    = 0;

        while (i < len) {
            final TrieNode<T> child = curr.getChild(getLookupChar(resource, i));

            if (child == null) {
                break;
            }

            final String childStr = child.getStr();

            if (!resource.regionMatches(optIgnoreCase, i, childStr, 0, childStr.length())) {
                break;
            }

            curr = child;
            i += childStr.length();
        }

        List<T> ret = i == len ? curr.getEvaluators() : curr.getWildcardEvaluators();

        RangerPerfTracer.logAlways(perf);

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerResourceTrie.getEvaluatorsForResource(" + resource + "): evaluatorCount=" + (ret == null ? 0 : ret.size()));
        }

        return ret;
    }

    private List<T> getEvaluatorsForResources(Collection<String> resources) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerResourceTrie.getEvaluatorsForResources(" + resources + ")");
        }

        List<T>      ret           = null;
        Map<Long, T> evaluatorsMap = null;

        for (String resource : resources) {
            List<T> resourceEvaluators = getEvaluatorsForResource(resource);

            if (CollectionUtils.isEmpty(resourceEvaluators)) {
                continue;
            }

            if (evaluatorsMap == null) {
                if (ret == null) { // first resource: don't create map yet
                    ret = resourceEvaluators;
                } else if (ret != resourceEvaluators) { // if evaluator list is same as earlier resources, retain the list, else create a map
                    evaluatorsMap = new HashMap<>();

                    for (T evaluator : ret) {
                        evaluatorsMap.put(evaluator.getId(), evaluator);
                    }

                    ret = null;
                }
            }

            if (evaluatorsMap != null) {
                for (T evaluator : resourceEvaluators) {
                    evaluatorsMap.put(evaluator.getId(), evaluator);
                }
            }
        }

        if (ret == null && evaluatorsMap != null) {
            ret = new ArrayList<>(evaluatorsMap.values());

            if (comparator != null) {
                ret.sort(comparator);
            }
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerResourceTrie.getEvaluatorsForResources(" + resources + "): evaluatorCount=" + (ret == null ? 0 : ret.size()));
        }

        return ret;
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

    class TrieData {
        int nodeCount;
        int leafNodeCount;
        int singleChildNodeCount;
        int maxDepth;
        int evaluatorListCount;
        int wildcardEvaluatorListCount;
        int evaluatorListRefCount;
        int wildcardEvaluatorListRefCount;
    }

    class TrieNode<U extends RangerPolicyResourceEvaluator> {
        private String str;
        private Map<Character, TrieNode<U>> children = new HashMap<>();
        private List<U> evaluators;
        private List<U> wildcardEvaluators;
        private boolean isSharingParentWildcardEvaluators;

        TrieNode(String str) {
            this.str = str;
        }

        String getStr() {
            return str;
        }

        void setStr(String str) {
            this.str = str;
        }

        Map<Character, TrieNode<U>> getChildren() {
            return children;
        }

        List<U> getEvaluators() {
            return evaluators;
        }

        List<U> getWildcardEvaluators() {
            return wildcardEvaluators;
        }

        TrieNode<U> getChild(Character ch) {
            return children == null ? null : children.get(ch);
        }

        void populateTrieData(RangerResourceTrie.TrieData trieData) {
            trieData.nodeCount++;

            if (wildcardEvaluators != null) {
                if (isSharingParentWildcardEvaluators) {
                    trieData.wildcardEvaluatorListRefCount++;
                } else {
                    trieData.wildcardEvaluatorListCount++;
                }
            }

            if (evaluators != null) {
                if (evaluators == wildcardEvaluators) {
                    trieData.evaluatorListRefCount++;
                } else {
                    trieData.evaluatorListCount++;
                }
            }

            if (children != null && !children.isEmpty()) {
                if (children.size() == 1) {
                    trieData.singleChildNodeCount++;
                }

                for (Map.Entry<Character, TrieNode<U>> entry : children.entrySet()) {
                    TrieNode child = entry.getValue();

                    child.populateTrieData(trieData);
                }
            } else {
                trieData.leafNodeCount++;
            }
        }

        int getMaxDepth() {
            int ret = 0;

            if (children != null) {
                for (Map.Entry<Character, TrieNode<U>> entry : children.entrySet()) {
                    TrieNode<U> child = entry.getValue();

                    int maxChildDepth = child.getMaxDepth();

                    if (maxChildDepth > ret) {
                        ret = maxChildDepth;
                    }
                }
            }

            return ret + 1;
        }

        TrieNode<U> getOrCreateChild(String str) {
            int len = str.length();

            TrieNode<U> child = children.get(getLookupChar(str, 0));

            if (child == null) {
                child = new TrieNode<>(str);
                addChild(child);
            } else {
                final String childStr = child.getStr();
                final int childStrLen = childStr.length();

                final boolean isExactMatch = optIgnoreCase ? StringUtils.equalsIgnoreCase(childStr, str) : StringUtils.equals(childStr, str);

                if (!isExactMatch) {
                    final int numOfCharactersToMatch = childStrLen < len ? childStrLen : len;
                    int index = 1;
                    for (; index < numOfCharactersToMatch; index++) {
                        if (getLookupChar(childStr, index) != getLookupChar(str, index)) {
                            break;
                        }
                    }
                    if (index == numOfCharactersToMatch) {
                        // Matched all
                        if (childStrLen > len) {
                            // Existing node has longer string, need to break up this node
                            TrieNode<U> newChild = new TrieNode<>(str);
                            this.addChild(newChild);
                            child.setStr(childStr.substring(index));
                            newChild.addChild(child);
                            child = newChild;
                        } else {
                            // This is a longer string, build a child with leftover string
                            child = child.getOrCreateChild(str.substring(index));
                        }
                    } else {
                        // Partial match for both; both have leftovers
                        String matchedPart = str.substring(0, index);
                        TrieNode<U> newChild = new TrieNode<>(matchedPart);
                        this.addChild(newChild);
                        child.setStr(childStr.substring(index));
                        newChild.addChild(child);
                        child = newChild.getOrCreateChild(str.substring(index));
                    }
                }
            }

            return child;
        }

        private void addChild(TrieNode<U> child) {
            children.put(getLookupChar(child.getStr(), 0), child);
        }

        void addEvaluator(U evaluator) {
            if (evaluators == null) {
                evaluators = new ArrayList<>();
            }

            if (!evaluators.contains(evaluator)) {
                evaluators.add(evaluator);
            }
        }

        void addWildcardEvaluator(U evaluator) {
            if (wildcardEvaluators == null) {
                wildcardEvaluators = new ArrayList<>();
            }

            if (!wildcardEvaluators.contains(evaluator)) {
                wildcardEvaluators.add(evaluator);
            }
        }

        void postSetup(List<U> parentWildcardEvaluators, Comparator<U> comparator) {
            // finalize wildcard-evaluators list by including parent's wildcard evaluators
            if (parentWildcardEvaluators != null) {
                if (CollectionUtils.isEmpty(this.wildcardEvaluators)) {
                    this.wildcardEvaluators = parentWildcardEvaluators;
                } else {
                    for (U evaluator : parentWildcardEvaluators) {
                        addWildcardEvaluator(evaluator);
                    }
                }
            }
            this.isSharingParentWildcardEvaluators = wildcardEvaluators == parentWildcardEvaluators;

            // finalize evaluators list by including wildcard evaluators
            if (wildcardEvaluators != null) {
                if (CollectionUtils.isEmpty(this.evaluators)) {
                    this.evaluators = wildcardEvaluators;
                } else {
                    for (U evaluator : wildcardEvaluators) {
                        addEvaluator(evaluator);
                    }
                }
            }

            if (comparator != null) {
                if (!isSharingParentWildcardEvaluators && CollectionUtils.isNotEmpty(wildcardEvaluators)) {
                    wildcardEvaluators.sort(comparator);
                }

                if (evaluators != wildcardEvaluators && CollectionUtils.isNotEmpty(evaluators)) {
                    evaluators.sort(comparator);
                }
            }

            if (children != null) {
                for (Map.Entry<Character, TrieNode<U>> entry : children.entrySet()) {
                    TrieNode<U> child = entry.getValue();

                    child.postSetup(wildcardEvaluators, comparator);
                }
            }
        }

        public void toString(String prefix, StringBuilder sb) {
            String nodeValue = prefix;

            if (str != null) {
                nodeValue += str;
            }

            sb.append("nodeValue=").append(nodeValue);
            sb.append("; childCount=").append(children == null ? 0 : children.size());
            sb.append("; evaluators=[ ");
            if (evaluators != null) {
                for (U evaluator : evaluators) {
                    sb.append(evaluator.getId()).append(" ");
                }
            }
            sb.append("]");

            sb.append("; wildcardEvaluators=[ ");
            if (wildcardEvaluators != null) {
                for (U evaluator : wildcardEvaluators) {
                    sb.append(evaluator.getId()).append(" ");
                }
            }
            sb.append("]\n");

            if (children != null) {
                for (Map.Entry<Character, TrieNode<U>> entry : children.entrySet()) {
                    TrieNode<U> child = entry.getValue();

                    child.toString(nodeValue, sb);
                }
            }
        }

        public void clear() {
            children = null;
            evaluators = null;
            wildcardEvaluators = null;
        }
    }
}
