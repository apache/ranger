package org.apache.hadoop.hbase.security.access;

import java.io.IOException;

import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.security.access.AccessControlLists;

public class XaAccessControlLists {
	public static void init(MasterServices master) throws IOException {
		AccessControlLists.init(master);
	}
}
