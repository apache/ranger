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

package org.apache.ranger.audit.producer.kafka.partition;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Parses and composes Kerberos {@code auth_to_local} rules from {@code ranger.audit.ingestor.auth.to.local}. */
public class AuthToLocalRuleCatalog {
    private static final Pattern RULE_SUBSTITUTION_SHORT_NAME_PATTERN =
            Pattern.compile("s/\\.\\*/([^/]+)/\\s*$");
    private static final String GENERATED_SHORT_NAME_RULE_TEMPLATE =
            "RULE:[2:$1/$2@$0](%s/.*@.*)s/.*/%s/";

    private final List<PrimaryCatalogRule> primaryCatalogRulesInOrder;
    private final List<String>           fallbackRuleLines;

    AuthToLocalRuleCatalog(List<PrimaryCatalogRule> primaryCatalogRulesInOrder, List<String> fallbackRuleLines) {
        this.primaryCatalogRulesInOrder = List.copyOf(primaryCatalogRulesInOrder);
        this.fallbackRuleLines          = List.copyOf(fallbackRuleLines);
    }

    public static AuthToLocalRuleCatalog parse(String rawAuthToLocalRules) {
        List<PrimaryCatalogRule> primaryCatalogRulesInOrder = new ArrayList<>();
        List<String>             fallbackRuleLines          = new ArrayList<>();
        for (String ruleLine : tokenizeRuleLines(rawAuthToLocalRules)) {
            if (isFallbackKerberosRule(ruleLine)) {
                fallbackRuleLines.add(ruleLine);
            } else {
                primaryCatalogRulesInOrder.add(new PrimaryCatalogRule(ruleLine, extractMappedShortName(ruleLine)));
            }
        }
        return new AuthToLocalRuleCatalog(primaryCatalogRulesInOrder, fallbackRuleLines);
    }

    /** Full static rule set (all primary catalog rules in order + fallback tail). */
    public String composeFullCatalogRules() {
        List<String> composedRuleLines = new ArrayList<>(primaryCatalogRulesInOrder.size() + fallbackRuleLines.size());
        for (PrimaryCatalogRule catalogRule : primaryCatalogRulesInOrder) {
            composedRuleLines.add(catalogRule.ruleLine);
        }
        composedRuleLines.addAll(fallbackRuleLines);
        return joinRuleLines(composedRuleLines);
    }

    /**
     * Builds the active rule set for dynamic mode: catalog rules whose mapped short name is in the union,
     * plus generated rules for union members not covered by the catalog, plus fallback tail.
     *
     * <p>Examples (union member to effective rule):
     * <ul>
     *   <li>{@code hdfs} in union: full hdfs catalog line (nn/dn/jn/hdfs principals map to hdfs)</li>
     *   <li>{@code nn} in union but catalog mapped name is hdfs: generated nn rule plus hdfs catalog line</li>
     *   <li>{@code foo} in union, not in catalog: generated foo/host@REALM to foo rule</li>
     * </ul>
     * Empty union falls back to {@link #composeFullCatalogRules()}.
     */
    public String composeRulesForAllowedShortNames(Set<String> allowedUserShortNames) {
        if (allowedUserShortNames == null || allowedUserShortNames.isEmpty()) {
            return composeFullCatalogRules();
        }

        Set<String> normalizedAllowedShortNames = allowedUserShortNames.stream()
                .filter(StringUtils::isNotBlank)
                .map(String::trim)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        if (normalizedAllowedShortNames.isEmpty()) {
            return composeFullCatalogRules();
        }

        List<String> composedRuleLines              = new ArrayList<>();
        Set<String>  shortNamesMatchedByCatalogRule = new LinkedHashSet<>();

        for (PrimaryCatalogRule catalogRule : primaryCatalogRulesInOrder) {
            if (catalogRule.mappedShortName != null && normalizedAllowedShortNames.contains(catalogRule.mappedShortName)) {
                composedRuleLines.add(catalogRule.ruleLine);
                shortNamesMatchedByCatalogRule.add(catalogRule.mappedShortName);
            }
        }

        List<String> generatedSimpleRuleLines = normalizedAllowedShortNames.stream()
                .filter(shortName -> !shortNamesMatchedByCatalogRule.contains(shortName))
                .sorted()
                .map(AuthToLocalRuleCatalog::buildGeneratedShortNameRule)
                .collect(Collectors.toList());
        composedRuleLines.addAll(generatedSimpleRuleLines);
        composedRuleLines.addAll(fallbackRuleLines);
        return joinRuleLines(composedRuleLines);
    }

    public int getPrimaryCatalogRuleCount() {
        return primaryCatalogRulesInOrder.size();
    }

    /**
     * Mapped Kerberos short name from a catalog rule line (substitution suffix s/.star/&lt;name&gt;/).
     * Example: hdfs catalog rule maps nn/dn/jn/hdfs principals to short name hdfs.
     */
    public static String extractMappedShortName(String ruleLine) {
        if (StringUtils.isBlank(ruleLine)) {
            return null;
        }
        Matcher substitutionMatcher = RULE_SUBSTITUTION_SHORT_NAME_PATTERN.matcher(ruleLine);
        if (substitutionMatcher.find()) {
            return substitutionMatcher.group(1);
        }
        return null;
    }

    private static List<String> tokenizeRuleLines(String rawAuthToLocalRules) {
        if (StringUtils.isBlank(rawAuthToLocalRules)) {
            return Collections.emptyList();
        }
        return Arrays.stream(rawAuthToLocalRules.split("\\s+"))
                .map(String::trim)
                .filter(ruleLine -> ruleLine.startsWith("RULE:") || "DEFAULT".equals(ruleLine))
                .collect(Collectors.toList());
    }

    private static boolean isFallbackKerberosRule(String ruleLine) {
        return "DEFAULT".equals(ruleLine) || ruleLine.startsWith("RULE:[1:") || ruleLine.contains("s/@.*//");
    }

    /** Simple rule for allowlist short names absent from the XML catalog: {@code myapp/host@REALM -> myapp}. */
    private static String buildGeneratedShortNameRule(String shortName) {
        return String.format(GENERATED_SHORT_NAME_RULE_TEMPLATE, shortName, shortName);
    }

    private static String joinRuleLines(List<String> ruleLines) {
        return String.join("\n", ruleLines);
    }
}
