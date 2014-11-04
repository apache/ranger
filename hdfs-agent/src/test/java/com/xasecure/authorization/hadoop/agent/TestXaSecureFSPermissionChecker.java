package com.xasecure.authorization.hadoop.agent;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.util.Set;

import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.XaSecureFSPermissionChecker;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;

import com.xasecure.authorization.hadoop.exceptions.XaSecureAccessControlException;

public class TestXaSecureFSPermissionChecker {

	@Test
	public void nullUgiToCheckReturnsFalse() {

		UserGroupInformation ugi = null;
		INode inode = null;
		FsAction access = null;
		try {
			boolean result = XaSecureFSPermissionChecker.check(ugi, inode, access);
			assertFalse(result);
		} catch (XaSecureAccessControlException e) {
			fail("Unexpected exception!");
		} 
	}
	
	@Test
	public void authorizeAccess() {
		String aPathName = null;
		String aPathOwnerName = null;
		String user = null;
		Set<String> groups = null;
		FsAction access = null;
		try {
			// null access returns false! 
			assertFalse(XaSecureFSPermissionChecker.AuthorizeAccessForUser(aPathName, aPathOwnerName, access, user, groups));
			// None access type returns true!
			access = FsAction.NONE;
			assertFalse(XaSecureFSPermissionChecker.AuthorizeAccessForUser(aPathName, aPathOwnerName, access, user, groups));
		} catch (XaSecureAccessControlException e) {
			e.printStackTrace();
			fail("Unexpected exception!");
		}
	}
}
