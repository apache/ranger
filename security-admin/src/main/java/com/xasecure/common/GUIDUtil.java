package com.xasecure.common;

import java.io.Serializable;
import java.security.SecureRandom;

public class GUIDUtil implements Serializable {
	
	private static final long serialVersionUID = -7284237762948427019L;

	static SecureRandom secureRandom = new SecureRandom();
	static int counter = 0;

	static public String genGUI() {
		return System.currentTimeMillis() + "_" + secureRandom.nextInt(1000)
				+ "_" + counter++;
	}

	public static long genLong() {
		return secureRandom.nextLong();
	}

}
