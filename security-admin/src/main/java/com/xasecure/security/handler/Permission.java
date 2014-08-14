package com.xasecure.security.handler;
public class Permission {

	public static final String CREATE_PERMISSION = "CREATE";
	public static final String READ_PERMISSION = "READ";
	public static final String UPDATE_PERMISSION = "UPDATE";
	public static final String DELETE_PERMISSION = "DELETE";

	public enum permissionType {
		CREATE, READ, UPDATE, DELETE
	};

	public static permissionType getPermisson(Object in) {
		String permString = in.toString();

		if (CREATE_PERMISSION.equals(permString)) {
			return permissionType.CREATE;
		}

		if (READ_PERMISSION.equals(permString)) {
			return permissionType.READ;
		}

		if (UPDATE_PERMISSION.equals(permString)) {
			return permissionType.UPDATE;
		}

		if (DELETE_PERMISSION.equals(permString)) {
			return permissionType.DELETE;
		}

		return null;
	}
}
