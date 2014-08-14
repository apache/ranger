package com.xasecure.pdp.hdfs;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.io.FilenameUtils;

public class AdminPolicyChecker {
		
	private  static final String PATH_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrst0123456789-_." ;
	private  static char[] PATH_CHAR_SET = PATH_CHARS.toCharArray() ; 
	private static int PATH_CHAR_SET_LEN = PATH_CHAR_SET.length ; 

	
	public static List<String> adminUserList = new ArrayList<String>() ;  // "cli@adfpros.com"
	public static List<String> adminGroupList = new ArrayList<String>()  ;
	
	static {
		adminUserList.add("cli@adfpros.com") ;
		adminGroupList.add("policymgradmin") ;
	}
	
	
	public void checkAdminAccessForResource(String selectedResourcePath, boolean isRecursiveFlag, String username) {
		
		if (adminUserList.contains(username)) {
			return ;  
		}
		
		List<String> groups = getUserGroupsForUser(username) ;
		
		if (adminGroupList.contains(groups)) {
			
		}
		
		checkAdminAccessForResource(new Path(selectedResourcePath, isRecursiveFlag), username) ;
	}

	private void checkAdminAccessForResource(Path resourcePath, String username) {

		List<Path> adminPathList = getAdminPathFromDB(username)  ;

		if (!adminPathList.isEmpty()) {
			for(Path adminPath : adminPathList ) {
				if (adminPath.isMatched(resourcePath)) {
					return  ;
				}
			}
		}

		throw new SecurityException("User [" + username + "]  does not have admin privileges on path [" + resourcePath + "]") ;

	}
	
	class Path {
		String fullPath ;
		boolean recursiveFlag ;

		Path(String fullPath, boolean recursiveFlag) {
			this.fullPath = fullPath;
			this.recursiveFlag = recursiveFlag;
		}

		public boolean isMatched(Path resourcePath) {
			// Since it is a Regular Expression Compared with Regular Expression
			// We will expand the resourcepath to a normalized form and see if it matches with the fullpath using a WildCardMatch
			// THIS IS JUST A WORK-AROUND. Need more permanent solution - 11/19/2013
			
			String expandedPath = repaceMetaChars(resourcePath) ;
			
			if (recursiveFlag) {
				return URLBasedAuthDB.isRecursiveWildCardMatch(expandedPath, fullPath) ;
			}
			else {
				return FilenameUtils.wildcardMatch(expandedPath, fullPath) ;
			}
		}
		
		private String repaceMetaChars(Path regEx) {
			
			String expandedPath = regEx.fullPath ;
			
			if (expandedPath.contains("*")) {
				String replacement = getRandomString(5,60) ;
				expandedPath.replaceAll("\\*", replacement) ;
			}
			
			if (expandedPath.contains("?")) {
				String replacement = getRandomString(1,1) ;
				expandedPath.replaceAll("\\?", replacement) ;
			}
			
			if (regEx.recursiveFlag) {
				int level = getRandomInt(3,10) ;
				if (! expandedPath.endsWith("/")) {
					expandedPath = expandedPath + "/" ;
				}
				expandedPath = expandedPath + getRandomString(5,60) ;
				
				for(int i = 1 ; i  < level ; i++) {
					expandedPath = expandedPath + "/" + getRandomString(5,60) ;
				}
			}
			return expandedPath ;
		}
		
		
		private Random random = new Random() ;

		private String getRandomString(int minLen, int maxLen) {
			StringBuilder sb = new StringBuilder() ;
			int len = getRandomInt(minLen,maxLen) ;
			for(int i = 0 ; i < len ; i++) {
				int charIdx = random.nextInt(PATH_CHAR_SET_LEN) ;
				sb.append( PATH_CHAR_SET[charIdx] ) ;
			}
			return null;
		}
		
		private int getRandomInt(int min, int max) {
			if (min == max) {
				return min ;
			}
			else {
				int interval = max - min ;
				return ((random.nextInt() % interval) + min) ;
			}
		}

	}
	
	
	private List<Path> getAdminPathFromDB(String username) {
		
		List<Path> ret = new ArrayList<Path>() ;

		//
		// TODO:  database work to get ACL ....
		//
		
		// Get all policy acl where the user has ADMIN permission +
		// Get all policy acl where group associated with user has ADMIN permission 
		// For each of the acl
		//	  For path in acl.getResourcePath().splitBy(",")
		//	     ret.add(new Path(path, acl.recursiveFlag)) ;
		
		return ret;
	}
	
	
	private List<String>  getUserGroupsForUser(String username) {
		List<String> groupList = new ArrayList<String>() ;

		//
		// TODO:  database work to get List of groups ....
		//

		return groupList ;
	}



}
