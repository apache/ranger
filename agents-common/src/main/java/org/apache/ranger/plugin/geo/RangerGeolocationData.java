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

package org.apache.ranger.plugin.geo;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class RangerGeolocationData implements Comparable<RangerGeolocationData>, RangeChecker<Long> {
	private static final Log LOG = LogFactory.getLog(RangerGeolocationData.class);

	private static final Character IPSegmentsSeparator = '.';

	private long fromIPAddress;
	private long toIPAddress;
	private String[] locationData = new String[0];

	public static RangerGeolocationData create(String fields[], int index, boolean useDotFormat) {

		RangerGeolocationData data = null;

		if (fields.length > 2) {
			String startAddress = fields[0];
			String endAddress = fields[1];

			if (RangerGeolocationData.validateAsIP(startAddress, useDotFormat) && RangerGeolocationData.validateAsIP(endAddress, useDotFormat)) {

				long startIP, endIP;
				if (useDotFormat) {
					startIP = RangerGeolocationData.ipAddressToLong(startAddress);
					endIP = RangerGeolocationData.ipAddressToLong(endAddress);
				} else {
					startIP = Long.valueOf(startAddress);
					endIP = Long.valueOf(endAddress);
				}

				if ((endIP - startIP) >= 0) {

					String[] locationData = new String[fields.length-2];
					for (int i = 2; i < fields.length; i++) {
						locationData[i-2] = fields[i];
					}
					data = new RangerGeolocationData(startIP, endIP, locationData);
				}
			}

		} else {
			LOG.error("GeolocationMetadata.createMetadata() - Not enough fields specified, need {start, end, location} at " + index);
		}
		return data;
	}

	public RangerGeolocationData(final long fromIPAddress, final long toIPAddress, final String[] locationData) {
		this.fromIPAddress = fromIPAddress;
		this.toIPAddress = toIPAddress;
		this.locationData = locationData;
	}

	public String[] getLocationData() {
		return locationData;
	}

	@Override
	public int compareTo(final RangerGeolocationData other) {
		int ret = Long.compare(fromIPAddress, other.fromIPAddress);
		if (ret == 0) {
			ret = Long.compare(toIPAddress, other.toIPAddress);
		}
		return ret;
	}

	@Override
	public int compareToRange(final Long ip) {
		int ret = Long.compare(fromIPAddress, ip.longValue());

		if (ret < 0) {
			ret = Long.compare(toIPAddress, ip.longValue());
			if (ret > 0) {
				ret = 0;
			}
		}

		return ret;
	}

	public static long ipAddressToLong(final String ipAddress) {

		long ret = 0L;

		try {
			byte[] bytes = InetAddress.getByName(ipAddress).getAddress();

			if (bytes != null && bytes.length <= 4) {
				for (int i = 0; i < bytes.length; i++) {
					int val = bytes[i] < 0 ? (256 + bytes[i]) : bytes[i];
					ret += (val << (8 * (3 - i)));
				}
			}
		}
		catch (UnknownHostException exception) {
			LOG.error("RangerGeolocationData.ipAddressToLong() - Invalid IP address " + ipAddress);
		}

		return ret;
	}

	public static String unsignedIntToIPAddress(final long val) {
		if (val <= 0) {
			return "";
		}
		long remaining = val;
		String segments[] = new String[4];
		for (int i = 3; i >= 0; i--) {
			long segment = remaining % 0x100;
			remaining = remaining / 0x100;
			segments[i] = String.valueOf(segment);
		}
		return StringUtils.join(segments, IPSegmentsSeparator);
	}

	public static boolean validateAsIP(String ipAddress, boolean ipInDotNotation) {
		if (!ipInDotNotation) {
			return StringUtils.isNumeric(ipAddress);
		}

		boolean ret = false;

		try {
			byte[] bytes = InetAddress.getByName(ipAddress).getAddress();
			ret = true;
		}
		catch(UnknownHostException exception) {
			LOG.error("RangerGeolocationData.validateAsIP() - Invalid address " + ipAddress);
		}

		return ret;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();

		toStringDump(sb);

		return sb.toString();
	}

	private StringBuilder toString(StringBuilder sb) {
		sb.append("{")
				.append("from=")
				.append(RangerGeolocationData.unsignedIntToIPAddress(fromIPAddress))
				.append(", to=")
				.append(RangerGeolocationData.unsignedIntToIPAddress(toIPAddress))
				.append(", location={");
			for (int i = 0; i < locationData.length; i++) {
				sb.append(locationData[i]).append(", ");
			}
				sb.append("}");
		sb.append("}");
		return sb;
	}

	private StringBuilder toStringDump(StringBuilder sb) {
		sb.append(RangerGeolocationData.unsignedIntToIPAddress(fromIPAddress))
				.append(",")
				.append(RangerGeolocationData.unsignedIntToIPAddress(toIPAddress))
				.append(",");
			for (int i = 0; i < locationData.length; i++) {
				sb.append(locationData[i]).append(", ");
			}
		return sb;
	}

}
