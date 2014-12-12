/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ranger.audit.provider;

import java.io.File;
import java.net.InetAddress;
import java.rmi.dgc.VMID;
import java.text.SimpleDateFormat;
import java.util.UUID;

import org.apache.log4j.helpers.LogLog;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class MiscUtil {
	public static final String TOKEN_START        = "%";
	public static final String TOKEN_END          = "%";
	public static final String TOKEN_HOSTNAME     = "hostname";
	public static final String TOKEN_APP_TYPE     = "app-type";
	public static final String TOKEN_JVM_INSTANCE = "jvm-instance";
	public static final String TOKEN_TIME         = "time:";
	public static final String TOKEN_PROPERTY     = "property:";
	public static final String TOKEN_ENV          = "env:";
	public static final String ESCAPE_STR           = "\\";

	static VMID sJvmID = new VMID();

	public static String LINE_SEPARATOR = System.getProperty("line.separator");

	private static Gson   sGsonBuilder = null;
	private static String sApplicationType = null;

	static {
		try {
			sGsonBuilder = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss.SSS").create();
		} catch(Throwable excp) {
			LogLog.warn("failed to create GsonBuilder object. stringigy() will return obj.toString(), instead of Json", excp);
		}
	}

	public static String replaceTokens(String str, long time) {
		if(str == null) {
			return str;
		}

		if(time <= 0) {
			time = System.currentTimeMillis();
		}

        for(int startPos = 0; startPos < str.length(); ) {
            int tagStartPos = str.indexOf(TOKEN_START, startPos);
            
            if(tagStartPos == -1) {
            	break;
            }

            int tagEndPos = str.indexOf(TOKEN_END, tagStartPos + TOKEN_START.length());

            if(tagEndPos == -1) {
            	break;
            }

            String tag   = str.substring(tagStartPos, tagEndPos+TOKEN_END.length());
            String token = tag.substring(TOKEN_START.length(), tag.lastIndexOf(TOKEN_END));
            String val   = "";

            if(token != null) {
	            if(token.equals(TOKEN_HOSTNAME)) {
	            	val = getHostname();
	            } else if(token.equals(TOKEN_APP_TYPE)) {
	            	val = getApplicationType();
	            } else if(token.equals(TOKEN_JVM_INSTANCE)) {
	            	val = getJvmInstanceId();
	            } else if(token.startsWith(TOKEN_PROPERTY)) {
	            	String propertyName = token.substring(TOKEN_PROPERTY.length());
	
	                val = getSystemProperty(propertyName);
	            } else if(token.startsWith(TOKEN_ENV)) {
	            	String envName = token.substring(TOKEN_ENV.length());
	
	                val = getEnv(envName);
	            } else if(token.startsWith(TOKEN_TIME)) {
	                String dtFormat = token.substring(TOKEN_TIME.length());
	                
	                val = getFormattedTime(time, dtFormat);
	            }
            }

            if(val == null) {
            	val = "";
            }

            str = str.substring(0, tagStartPos) + val + str.substring(tagEndPos + TOKEN_END.length());
            startPos = tagStartPos + val.length();
        }

        return str;
	}

	public static String getHostname() {
		String ret = null;

		try {
			ret = InetAddress.getLocalHost().getHostName();
		} catch (Exception excp) {
			LogLog.warn("getHostname()", excp);
		}

		return ret;
	}

	public static void setApplicationType(String applicationType) {
		sApplicationType = applicationType;
	}

	public static String getApplicationType() {
		return sApplicationType;
	}

	public static String getJvmInstanceId() {
		String ret = Integer.toString(Math.abs(sJvmID.toString().hashCode()));

		return ret;
	}

	public static String getSystemProperty(String propertyName) {
		String ret = null;

		try {
			ret = propertyName != null ? System.getProperty(propertyName) : null;
		} catch (Exception excp) {
			LogLog.warn("getSystemProperty(" + propertyName + ") failed", excp);
		}

		return ret;
	}

	public static String getEnv(String envName) {
		String ret = null;

		try {
			ret = envName != null ? System.getenv(envName) : null;
		} catch (Exception excp) {
			LogLog.warn("getenv(" + envName + ") failed", excp);
		}

		return ret;
	}

	public static String getFormattedTime(long time, String format) {
		String ret = null;

		try {
            SimpleDateFormat sdf = new SimpleDateFormat(format);

            ret = sdf.format(time);
		} catch (Exception excp) {
			LogLog.warn("SimpleDateFormat.format() failed: " + format, excp);
		}

		return ret;
	}

	public static void createParents(File file) {
		if(file != null) {
			String parentName = file.getParent();

			if (parentName != null) {
				File parentDir = new File(parentName);

				if(!parentDir.exists()) {
					if(! parentDir.mkdirs()) {
						LogLog.warn("createParents(): failed to create " + parentDir.getAbsolutePath());
					}
				}
			}
		}
	}

	public static long getNextRolloverTime(long lastRolloverTime, long interval) {
		long now = System.currentTimeMillis() / 1000 * 1000; // round to second

		if(lastRolloverTime <= 0) {
			// should this be set to the next multiple-of-the-interval from start of the day?
			return now + interval;
		} else if(lastRolloverTime <= now) {
			long nextRolloverTime = now + interval;

			// keep it at 'interval' boundary
			long trimInterval = (nextRolloverTime - lastRolloverTime) % interval;

			return nextRolloverTime - trimInterval;
		} else {
			return lastRolloverTime;
		}
	}

	public static long getRolloverStartTime(long nextRolloverTime, long interval) {
		return (nextRolloverTime <= interval) ? System.currentTimeMillis() : nextRolloverTime - interval;
	}

	public static int parseInteger(String str, int defValue) {
		int ret = defValue;

		if(str != null) {
			try {
				ret = Integer.parseInt(str);
			} catch(Exception excp) {
				// ignore
			}
		}

		return ret;
	}
	
	public static String generateUniqueId() {
		return UUID.randomUUID().toString();
	}

	public static <T> String stringify(T log) {
		String ret = null;

		if(log != null) {
			if(log instanceof String) {
				ret = (String)log;
			} else if(MiscUtil.sGsonBuilder != null) {
				ret = MiscUtil.sGsonBuilder.toJson(log);
			} else {
				ret = log.toString();
			}
		}

		return ret;
	}
}
