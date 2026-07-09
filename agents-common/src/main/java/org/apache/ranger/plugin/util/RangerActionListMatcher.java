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
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

public class RangerActionListMatcher {
    private final boolean     allowAnyAction;
    private final Set<String> exactActions = new HashSet<>();
    private final String[]    prefixActions;

    public RangerActionListMatcher(Collection<String> actions) {
        boolean            allowAny       = CollectionUtils.isEmpty(actions);
        final List<String> tempPrefixList = new ArrayList<>();

        if (!allowAny) {
            for (String a : actions) {
                final String action = StringUtils.trimToEmpty(a);

                if (action.isEmpty()) {
                    continue;
                }

                if (action.equals("*")) {
                    allowAny = true;
                    exactActions.clear();
                    tempPrefixList.clear();
                    break;
                }

                if (action.endsWith("*")) {
                    final String prefix = StringUtils.trimToEmpty(action.substring(0, action.length() - 1)).toLowerCase(Locale.ROOT);

                    if (prefix.isEmpty()) {
                        allowAny = true;
                        exactActions.clear();
                        tempPrefixList.clear();
                        break;
                    }

                    tempPrefixList.add(prefix);
                } else {
                    exactActions.add(action.toLowerCase(Locale.ROOT));
                }
            }

            if (!allowAny && exactActions.isEmpty() && tempPrefixList.isEmpty()) {
                allowAny = true;
            }
        }

        this.allowAnyAction = allowAny;

        if (!tempPrefixList.isEmpty()) {
            tempPrefixList.sort((s1, s2) -> Integer.compare(s1.length(), s2.length()));

            List<String> optimizedPrefixes = new ArrayList<>();
            for (String p : tempPrefixList) {
                boolean isCovered = false;
                for (String optPrefix : optimizedPrefixes) {
                    if (p.startsWith(optPrefix)) {
                        isCovered = true;
                        break;
                    }
                }
                if (!isCovered) {
                    optimizedPrefixes.add(p);
                }
            }
            this.prefixActions = optimizedPrefixes.toArray(new String[0]);
        } else {
            this.prefixActions = new String[0];
        }
    }

    public boolean isMatch(String requestAction) {
        if (allowAnyAction) {
            return true;
        }

        // Action restrictions exist; missing request action is not a match.
        if (StringUtils.isBlank(requestAction)) {
            return false;
        }

        final String actionLower = requestAction.toLowerCase(Locale.ROOT);

        if (exactActions.contains(actionLower)) {
            return true;
        }

        for (String prefix : prefixActions) {
            if (actionLower.startsWith(prefix)) {
                return true;
            }
        }

        return false;
    }
}
