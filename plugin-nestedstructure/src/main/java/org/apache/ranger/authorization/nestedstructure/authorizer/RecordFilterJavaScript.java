/**
 * Copyright 2022 Comcast Cable Communications Management, LLC
 * <p>
 * Licensed under the Apache License, Version 2.0 (the ""License"");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an ""AS IS"" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or   implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * <p>
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.ranger.authorization.nestedstructure.authorizer;

import org.apache.ranger.plugin.util.GraalScriptEngineCreator;
import org.apache.ranger.plugin.util.ScriptEngineUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import javax.script.ScriptEngine;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * Executes a row-filter expression to determine if the user has access to the selected record.
 * <p>
 * The engine comes from {@link GraalScriptEngineCreator#createNoHostAccessScriptEngine()}, which exposes no host
 * classes and no host methods. Evaluation fails when that engine cannot be created.
 * </p>
 */
public class RecordFilterJavaScript {
    private static final Logger logger = LoggerFactory.getLogger(RecordFilterJavaScript.class);

    private RecordFilterJavaScript() {
    }

    /**
     * javascript primitive imports that the nashorn engine needs to function properly, e.g., with "includes"
     */
    private static final String NASHORN_POLYFILL_ARRAY_PROTOTYPE_INCLUDES = "if (!Array.prototype.includes) " +
            "{ Object.defineProperty(Array.prototype, 'includes', { value: function(valueToFind, fromIndex) " +
            "{ if (this == null) { throw new TypeError('\"this\" is null or not defined'); } var o = Object(this); " +
            "var len = o.length >>> 0; if (len === 0) { return false; } var n = fromIndex | 0; " +
            "var k = Math.max(n >= 0 ? n : len - Math.abs(n), 0); " +
            "function sameValueZero(x, y) { return x === y || (typeof x === 'number' && typeof y === 'number' " +
            "&& isNaN(x) && isNaN(y)); } while (k < len) { if (sameValueZero(o[k], valueToFind)) { return true; } k++; }" +
            " return false; } }); }";

    /**
     * Nashorn mixed Java and JS, so many filters use {@code str.equals('x')}. ECMAScript strings (and Graal.js) use
     * {@code ===} instead; this shim makes {@code .equals} behave like {@code String.equals} for string operands.
     */
    private static final String NASHORN_STYLE_STRING_EQUALS_SHIM = "if (typeof String.prototype.equals !== 'function') { " +
            "String.prototype.equals = function (other) { " +
            "  if (other == null) { return false; } " +
            "  return String(this) === String(other); " +
            "}; " +
            "}";

    public static boolean filterRow(String user, String filterExpr, String jsonString) {
        SecurityFilter securityFilter = new SecurityFilter();

        if (securityFilter.containsMalware(filterExpr)) {
            throw new MaskingException("cannot process filter expression: blocked by script safety checks: " + filterExpr);
        }

        ScriptEngine engine = GraalScriptEngineCreator.createNoHostAccessScriptEngine();

        if (engine == null) {
            throw new MaskingException("unable to evaluate filter expression: script engine is not available");
        }

        logger.debug("filterExpr: {}", filterExpr);

        // convert the given JSON string to JavaScript object, which the filterExpr expects, and then exec the filterExpr
        String script = ScriptEngineUtil.SCRIPT_SAFE_PREEXEC
                + " var jsonAttr = JSON.parse(jsonString); "
                + NASHORN_STYLE_STRING_EQUALS_SHIM
                + " "
                + NASHORN_POLYFILL_ARRAY_PROTOTYPE_INCLUDES
                + " "
                + filterExpr;

        Bindings bindings = null;

        try {
            bindings = engine.createBindings();

            bindings.put("jsonString", jsonString);
            bindings.put("user", user);

            Object  raw       = engine.eval(script, bindings);
            boolean hasAccess = toScriptBooleanResult(raw);

            logger.debug("row filter access={}", hasAccess);

            return hasAccess;
        } catch (Exception e) {
            throw new MaskingException("unable to properly evaluate filter expression: " + filterExpr, e);
        } finally {
            closeQuietly(bindings);
            GraalScriptEngineCreator.closeScriptEngine(engine);
        }
    }

    private static void closeQuietly(Object resource) {
        if (resource instanceof AutoCloseable) {
            try {
                ((AutoCloseable) resource).close();
            } catch (Exception e) {
                logger.debug("failed to close script engine resource", e);
            }
        }
    }

    /**
     * Graal/Truffle and Nashorn can return different types for a JS boolean expression; normalize for {@code filterRow}.
     */
    private static boolean toScriptBooleanResult(Object value) {
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        if (value == null) {
            return false;
        }
        if (value instanceof Number) {
            return ((Number) value).doubleValue() != 0.0d;
        }

        return Boolean.parseBoolean(String.valueOf(value));
    }

    /**
     * This class filter prevents javascript from importing, using or reflecting any java classes
     * Helps keep javascript clean of injections.  It also contains other checks to ensure that injected
     * javascript is reasonably safe.
     */

    static class SecurityFilter {
        /**
         * Substrings (checked case-insensitively) that indicate script engine escape, Java interop, or other unsafe
         * patterns. This is a supplement to engine-level host-access restrictions, not a full Nashorn ClassFilter
         * replacement.
         */
        private static final List<String> FORBIDDEN_SUBSTRINGS = Arrays.asList(
                "this.engine",
                "java.type",
                "java.extend",
                "packages.",
                "loadwithnewglobal",
                "__nosuchproperty__",
                "factory.scriptengine",
                "com.sun.",
                "org.graalvm",
                "jdk.internal",
                "javax.script");

        /**
         * @param filterExpr the javascript to check if it contains potentially harmful commands
         * @return if this script is likely bad
         */
        boolean containsMalware(String filterExpr) {
            if (filterExpr == null || filterExpr.isEmpty()) {
                return false;
            }
            String n = filterExpr.toLowerCase(Locale.ROOT);
            for (String bad : FORBIDDEN_SUBSTRINGS) {
                if (n.contains(bad)) {
                    return true;
                }
            }
            return false;
        }
    }
}
