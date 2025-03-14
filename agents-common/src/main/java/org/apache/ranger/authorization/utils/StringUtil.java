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

package org.apache.ranger.authorization.utils;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class StringUtil {
    private static final TimeZone gmtTimeZone = TimeZone.getTimeZone("GMT+0");

    private StringUtil() {
        // to block instantiation
    }

    public static boolean equals(String str1, String str2) {
        final boolean ret;

        if (str1 == null) {
            ret = str2 == null;
        } else if (str2 == null) {
            ret = false;
        } else {
            ret = str1.equals(str2);
        }

        return ret;
    }

    public static boolean equalsIgnoreCase(String str1, String str2) {
        final boolean ret;

        if (str1 == null) {
            ret = str2 == null;
        } else if (str2 == null) {
            ret = false;
        } else {
            ret = str1.equalsIgnoreCase(str2);
        }

        return ret;
    }

    public static boolean equals(Collection<String> set1, Collection<String> set2) {
        final boolean ret;

        if (set1 == null) {
            ret = set2 == null;
        } else if (set2 == null) {
            ret = false;
        } else if (set1.size() == set2.size()) {
            ret = set1.containsAll(set2);
        } else {
            ret = false;
        }

        return ret;
    }

    public static boolean equalsIgnoreCase(Collection<String> set1, Collection<String> set2) {
        final boolean ret;

        if (set1 == null) {
            ret = set2 == null;
        } else if (set2 == null) {
            ret = false;
        } else if (set1.size() == set2.size()) {
            int numFound = 0;

            for (String str1 : set1) {
                boolean str1Found = false;

                for (String str2 : set2) {
                    if (equalsIgnoreCase(str1, str2)) {
                        str1Found = true;

                        break;
                    }
                }

                if (str1Found) {
                    numFound++;
                } else {
                    break;
                }
            }

            ret = numFound == set1.size();
        } else {
            ret = false;
        }

        return ret;
    }

    public static boolean matches(String pattern, String str) {
        final boolean ret;

        if (pattern == null || str == null || pattern.isEmpty() || str.isEmpty()) {
            ret = true;
        } else {
            ret = str.matches(pattern);
        }

        return ret;
    }

    public static boolean contains(String str, String strToFind) {
        return str != null && strToFind != null && str.contains(strToFind);
    }

    public static boolean containsIgnoreCase(String str, String strToFind) {
        return str != null && strToFind != null && str.toLowerCase().contains(strToFind.toLowerCase());
    }

    public static boolean contains(String[] strArr, String str) {
        boolean ret = false;

        if (strArr != null && strArr.length > 0 && str != null) {
            for (String s : strArr) {
                ret = equals(s, str);

                if (ret) {
                    break;
                }
            }
        }

        return ret;
    }

    public static boolean containsIgnoreCase(String[] strArr, String str) {
        boolean ret = false;

        if (strArr != null && strArr.length > 0 && str != null) {
            for (String s : strArr) {
                ret = equalsIgnoreCase(s, str);

                if (ret) {
                    break;
                }
            }
        }

        return ret;
    }

    public static String toString(Iterable<String> iterable) {
        String ret = "";

        if (iterable != null) {
            int count = 0;

            for (String str : iterable) {
                if (count == 0) {
                    ret = str;
                } else {
                    ret += (", " + str);
                }

                count++;
            }
        }

        return ret;
    }

    public static String toString(String[] arr) {
        String ret = "";

        if (arr != null && arr.length > 0) {
            ret = arr[0];

            for (int i = 1; i < arr.length; i++) {
                ret += (", " + arr[i]);
            }
        }

        return ret;
    }

    public static String toString(List<String> arr) {
        String ret = "";

        if (arr != null && !arr.isEmpty()) {
            ret = arr.get(0);

            for (int i = 1; i < arr.size(); i++) {
                ret += (", " + arr.get(i));
            }
        }

        return ret;
    }

    public static boolean isEmpty(String str) {
        return str == null || str.trim().isEmpty();
    }

    public static <T> boolean isEmpty(Collection<T> set) {
        return set == null || set.isEmpty();
    }

    public static String toLower(String str) {
        return str == null ? null : str.toLowerCase();
    }

    public static byte[] getBytes(String str) {
        return str == null ? null : str.getBytes();
    }

    public static Date getUTCDate() {
        Calendar          local  = Calendar.getInstance();
        int               offset = local.getTimeZone().getOffset(local.getTimeInMillis());
        GregorianCalendar utc    = new GregorianCalendar(gmtTimeZone);

        utc.setTimeInMillis(local.getTimeInMillis());
        utc.add(Calendar.MILLISECOND, -offset);

        return utc.getTime();
    }

    public static Date getUTCDateForLocalDate(Date date) {
        Calendar          local  = Calendar.getInstance();
        int               offset = local.getTimeZone().getOffset(local.getTimeInMillis());
        GregorianCalendar utc    = new GregorianCalendar(gmtTimeZone);

        utc.setTimeInMillis(date.getTime());
        utc.add(Calendar.MILLISECOND, -offset);

        return utc.getTime();
    }

    public static Map<String, Object> toStringObjectMap(Map<String, String> map) {
        Map<String, Object> ret = null;

        if (map != null) {
            ret = new HashMap<>(map.size());

            for (Map.Entry<String, String> e : map.entrySet()) {
                ret.put(e.getKey(), e.getValue());
            }
        }

        return ret;
    }

    public static Set<String> toSet(String str) {
        Set<String> values = new HashSet<>();

        if (StringUtils.isNotBlank(str)) {
            for (String item : str.split(",")) {
                if (StringUtils.isNotBlank(item)) {
                    values.add(StringUtils.trim(item));
                }
            }
        }

        return values;
    }

    public static List<String> toList(String str) {
        final List<String> values;

        if (StringUtils.isNotBlank(str)) {
            values = new ArrayList<>();

            for (String item : str.split(",")) {
                if (StringUtils.isNotBlank(item)) {
                    values.add(StringUtils.trim(item));
                }
            }
        } else {
            values = Collections.emptyList();
        }

        return values;
    }

    public static List<String> getURLs(String configURLs) {
        List<String> configuredURLs = new ArrayList<>();

        if (configURLs != null) {
            String[] urls = configURLs.split(",");

            for (String strUrl : urls) {
                if (StringUtils.isNotEmpty(StringUtils.trimToEmpty(strUrl))) {
                    if (strUrl.endsWith("/")) {
                        strUrl = strUrl.substring(0, strUrl.length() - 1);
                    }

                    configuredURLs.add(strUrl);
                }
            }
        }

        return configuredURLs;
    }

    public static Map<String, Map<String, String>> dedupStringsMapOfMap(Map<String, Map<String, String>> value, Map<String, String> strTbl) {
        final Map<String, Map<String, String>> ret;

        if (MapUtils.isNotEmpty(value)) {
            ret = new HashMap<>(value.size());

            for (Map.Entry<String, Map<String, String>> entry : value.entrySet()) {
                ret.put(dedupString(entry.getKey(), strTbl), dedupStringsMap(entry.getValue(), strTbl));
            }
        } else {
            ret = value;
        }

        return ret;
    }

    public static Map<String, Set<String>> dedupStringsMapOfSet(Map<String, Set<String>> value, Map<String, String> strTbl) {
        final Map<String, Set<String>> ret;

        if (MapUtils.isNotEmpty(value)) {
            ret = new HashMap<>(value.size());

            for (Map.Entry<String, Set<String>> entry : value.entrySet()) {
                ret.put(dedupString(entry.getKey(), strTbl), dedupStringsSet(entry.getValue(), strTbl));
            }
        } else {
            ret = value;
        }

        return ret;
    }

    public static Map<String, List<String>> dedupStringsMapOfList(Map<String, List<String>> value, Map<String, String> strTbl) {
        final Map<String, List<String>> ret;

        if (MapUtils.isNotEmpty(value)) {
            ret = new HashMap<>(value.size());

            for (Map.Entry<String, List<String>> entry : value.entrySet()) {
                ret.put(dedupString(entry.getKey(), strTbl), dedupStringsList(entry.getValue(), strTbl));
            }
        } else {
            ret = value;
        }

        return ret;
    }

    public static HashMap<String, List<String>> dedupStringsHashMapOfList(HashMap<String, List<String>> value, Map<String, String> strTbl) {
        final HashMap<String, List<String>> ret;

        if (MapUtils.isNotEmpty(value)) {
            ret = new HashMap<>(value.size());

            for (Map.Entry<String, List<String>> entry : value.entrySet()) {
                ret.put(dedupString(entry.getKey(), strTbl), dedupStringsList(entry.getValue(), strTbl));
            }
        } else {
            ret = value;
        }

        return ret;
    }

    public static Map<String, Object> dedupStringsMapOfObject(Map<String, Object> value, Map<String, String> strTbl) {
        final Map<String, Object> ret;

        if (MapUtils.isNotEmpty(value)) {
            ret = new HashMap<>(value.size());

            for (Map.Entry<String, Object> entry : value.entrySet()) {
                ret.put(dedupString(entry.getKey(), strTbl), entry.getValue());
            }
        } else {
            ret = value;
        }

        return ret;
    }

    public static Map<String, RangerPolicyResource> dedupStringsMapOfPolicyResource(Map<String, RangerPolicyResource> value, Map<String, String> strTbl) {
        final Map<String, RangerPolicyResource> ret;

        if (MapUtils.isNotEmpty(value)) {
            ret = new HashMap<>(value.size());

            for (Map.Entry<String, RangerPolicyResource> entry : value.entrySet()) {
                RangerPolicyResource resource = entry.getValue();

                resource.dedupStrings(strTbl);

                ret.put(dedupString(entry.getKey(), strTbl), resource);
            }
        } else {
            ret = value;
        }

        return ret;
    }

    public static Map<String, String> dedupStringsMap(Map<String, String> value, Map<String, String> strTbl) {
        final Map<String, String> ret;

        if (MapUtils.isNotEmpty(value)) {
            ret = new HashMap<>(value.size());

            for (Map.Entry<String, String> entry : value.entrySet()) {
                ret.put(dedupString(entry.getKey(), strTbl), dedupString(entry.getValue(), strTbl));
            }
        } else {
            ret = value;
        }

        return ret;
    }

    public static Set<String> dedupStringsSet(Set<String> value, Map<String, String> strTbl) {
        final Set<String> ret;

        if (CollectionUtils.isNotEmpty(value)) {
            ret = new HashSet<>(value.size());

            for (String val : value) {
                ret.add(dedupString(val, strTbl));
            }
        } else {
            ret = value;
        }

        return ret;
    }

    public static List<String> dedupStringsList(List<String> value, Map<String, String> strTbl) {
        final List<String> ret;

        if (CollectionUtils.isNotEmpty(value)) {
            ret = new ArrayList<>(value.size());

            for (String val : value) {
                ret.add(dedupString(val, strTbl));
            }
        } else {
            ret = value;
        }

        return ret;
    }

    public static Collection<String> dedupStringsCollection(Collection<String> value, Map<String, String> strTbl) {
        final Collection<String> ret;

        if (CollectionUtils.isNotEmpty(value)) {
            ret = value instanceof Set ? new HashSet<>(value.size()) : new ArrayList<>(value.size());

            for (String val : value) {
                ret.add(dedupString(val, strTbl));
            }
        } else {
            ret = value;
        }

        return ret;
    }

    public static String dedupString(String str, Map<String, String> strTbl) {
        String ret = str != null ? strTbl.putIfAbsent(str, str) : null;

        return ret == null ? str : ret;
    }

    public static String compressString(String input) throws IOException {
        final String ret;

        if (StringUtils.isEmpty(input)) {
            ret = input;
        } else {
            ret = new String(gzipCompress(input), StandardCharsets.ISO_8859_1);
        }

        return ret;
    }

    public static String decompressString(String input) throws IOException {
        final String ret;

        if (StringUtils.isEmpty(input)) {
            ret = input;
        } else {
            ret = gzipDecompress(input.getBytes(StandardCharsets.ISO_8859_1));
        }

        return ret;
    }

    public static byte[] gzipCompress(String input) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        GZIPOutputStream      gos = new GZIPOutputStream(out);

        gos.write(input.getBytes(StandardCharsets.ISO_8859_1));
        gos.close();

        return out.toByteArray();
    }

    public static String gzipDecompress(byte[] input) throws IOException {
        ByteArrayInputStream  in  = new ByteArrayInputStream(input);
        GZIPInputStream       gis = new GZIPInputStream(in);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[]                buf = new byte[1024];

        while (true) {
            int bytesRead = gis.read(buf, 0, buf.length);

            if (bytesRead == -1) {
                break;
            }

            out.write(buf, 0, bytesRead);
        }

        gis.close();

        return new String(out.toByteArray(), StandardCharsets.ISO_8859_1);
    }
}
