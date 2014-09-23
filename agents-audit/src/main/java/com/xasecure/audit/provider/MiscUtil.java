package com.xasecure.audit.provider;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.dgc.VMID;
import java.text.SimpleDateFormat;

import org.apache.log4j.helpers.LogLog;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class MiscUtil {
	public static final String TOKEN_HOSTNAME     = "%hostname%";
	public static final String TOKEN_JVM_INSTANCE = "%jvm-instance%";
	public static final String TOKEN_TIME_START   = "%time:";
	public static final String TOKEN_TIME_END     = "%";
	public static final String ESCAPE_STR         = "\\";

	static VMID sJvmID = new VMID();

	public static String LINE_SEPARATOR = System.getProperty("line.separator");

	private static Gson sGsonBuilder = null;

	static {
		try {
			sGsonBuilder = new GsonBuilder().create();
		} catch(Throwable excp) {
			LogLog.warn("failed to create GsonBuilder object. stringigy() will return obj.toString(), instead of Json", excp);
		}
	}

	public static String replaceTokens(String str, long time) {
		if(str == null) {
			return str;
		}

		str = replaceHostname(str);
		str = replaceJvmInstance(str);
		str = replaceTime(str, time);

		return str;
	}

	public static String replaceHostname(String str) {
		if(!str.contains(TOKEN_HOSTNAME)) {
			return str;
		}

		String hostName = null;

		try {
			hostName = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException excp) {
			LogLog.warn("replaceHostname()", excp);
		}

		if(hostName == null) {
			hostName = "Unknown";
		}

		return str.replace(TOKEN_HOSTNAME, hostName);
	}
	
	public static String replaceJvmInstance(String str) {
		if(!str.contains(TOKEN_JVM_INSTANCE)) {
			return str;
		}

		String jvmInstance = Integer.toString(Math.abs(sJvmID.hashCode()));

		return str.replace(TOKEN_JVM_INSTANCE, jvmInstance);
	}

	public static String replaceTime(String str, long time) {
		if(time <= 0) {
			time = System.currentTimeMillis();
		}

        while(str.contains(TOKEN_TIME_START)) {
            int tagStartPos = str.indexOf(TOKEN_TIME_START);
            int tagEndPos   = str.indexOf(TOKEN_TIME_END, tagStartPos + TOKEN_TIME_START.length());

            if(tagEndPos <= tagStartPos) {
            	break;
            }

            String tag      = str.substring(tagStartPos, tagEndPos+1);
            String dtFormat = tag.substring(TOKEN_TIME_START.length(), tag.lastIndexOf(TOKEN_TIME_END));

            String replaceStr = "";

            if(dtFormat != null) {
                SimpleDateFormat sdf = new SimpleDateFormat(dtFormat);

                replaceStr = sdf.format(time);
            }

            str = str.replace(tag, replaceStr);
        }

        return str;
	}

	public static void createParents(File file) {
		if(file != null) {
			String parentName = file.getParent();

			if (parentName != null) {
				File parentDir = new File(parentName);

				if(!parentDir.exists()) {
					parentDir.mkdirs();
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
