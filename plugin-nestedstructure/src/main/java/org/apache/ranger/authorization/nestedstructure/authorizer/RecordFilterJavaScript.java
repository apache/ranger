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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Executes an injected javascript command to determine if the user has access to the selected record.
 * <p>
 * Prefers the {@code graal.js} engine when present; otherwise falls back to Nashorn or other {@code javax.script}
 * engines on the classpath. GraalJS is configured with {@code polyglot.js.allowHostAccess} <strong>disabled by
 * default</strong> so record-filter scripts cannot access Java classes or host services. To allow Java interop (not
 * recommended), set the system property {@value #RANGER_RECORDFILTER_JS_ALLOW_HOST_ACCESS} to {@code true} on the JVM
 * (Graal only).
 * </p>
 */
public class RecordFilterJavaScript {
    private static final Logger logger = LoggerFactory.getLogger(RecordFilterJavaScript.class);

    /**
     * When set to {@code true}, enables {@code polyglot.js.allowHostAccess} for the GraalJS engine used by record
     * filters. Default is off; this must be an explicit, reviewed decision.
     */
    public static final String RANGER_RECORDFILTER_JS_ALLOW_HOST_ACCESS = "ranger.nestedstructure.recordfilter.js.allowHostAccess";

    private static final AtomicBoolean LOGGED_HOST_ACCESS_WARNING = new AtomicBoolean();

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

        ClassLoader clsLoader = Thread.currentThread().getContextClassLoader();
        ScriptEngineManager mgr = new ScriptEngineManager(clsLoader);
        ScriptEngine        engine = resolveJavaScriptEngine(mgr);

        if (isGraalJsEngine(engine)) {
            try {
                Map<String, Boolean> graalVmConfigs = new HashMap<>();

                boolean allowHost = Boolean.parseBoolean(
                        System.getProperty(RANGER_RECORDFILTER_JS_ALLOW_HOST_ACCESS, "false"));
                graalVmConfigs.put("polyglot.js.allowHostAccess", allowHost);
                if (allowHost && LOGGED_HOST_ACCESS_WARNING.compareAndSet(false, true)) {
                    logger.warn(
                            "{}=true: GraalJS host/Java interop is enabled for nested-structure record filter scripts. "
                                    + "Use only in a tightly controlled environment.",
                            RANGER_RECORDFILTER_JS_ALLOW_HOST_ACCESS);
                }
                graalVmConfigs.put("polyglot.js.nashorn-compat", Boolean.TRUE);

                Bindings engBindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
                engBindings.putAll(graalVmConfigs);
                engine.setBindings(engBindings, ScriptContext.ENGINE_SCOPE);
            } catch (Throwable t) {
                logger.warn("RecordFilterJavaScript.filterRow(): failed to apply GraalJS options for engine graal.js", t);
            }
        }

        if (engine == null) {
            throw new MaskingException("No JavaScript engine (graal.js, nashorn, or js) is available on the classpath");
        }

        logger.debug("filterExpr: {}", filterExpr);

        // convert the given JSON string to JavaScript object, which the filterExpr expects, and then exec the filterExpr
        String script = " var jsonAttr = JSON.parse(jsonString); "
                + NASHORN_STYLE_STRING_EQUALS_SHIM
                + " "
                + NASHORN_POLYFILL_ARRAY_PROTOTYPE_INCLUDES
                + " "
                + filterExpr;

        try {
            Bindings bindings = engine.createBindings();

            bindings.put("jsonString", jsonString);
            bindings.put("user", user);

            Object  raw       = engine.eval(script, bindings);
            boolean hasAccess = toScriptBooleanResult(raw);

            logger.debug("row filter access={}", hasAccess);

            return hasAccess;
        } catch (Exception e) {
            throw new MaskingException("unable to properly evaluate filter expression: " + filterExpr, e);
        }
    }

    private static ScriptEngine resolveJavaScriptEngine(ScriptEngineManager mgr) {
        String[] tryNames = {"graal.js", "nashorn", "js", "JavaScript", "javascript"};

        for (String name : tryNames) {
            ScriptEngine engine = mgr.getEngineByName(name);

            if (engine != null) {
                return engine;
            }
        }

        return null;
    }

    private static boolean isGraalJsEngine(ScriptEngine engine) {
        if (engine == null) {
            return false;
        }

        try {
            return "graal.js".equalsIgnoreCase(engine.getFactory().getEngineName());
        } catch (Exception e) {
            return false;
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
