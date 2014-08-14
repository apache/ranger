package com.xasecure.pdp.model;

public class ResourcePath {
	
	String path ;
	boolean wildcardPath ;
	
	public ResourcePath(String path) {
		this.path = path ;
		if (this.path.contains("*") || this.path.contains("?")) {
			this.wildcardPath = true ;
		}
	}

	public String getPath() {
		return path;
	}

	public boolean isWildcardPath() {
		return wildcardPath;
	}
	

}
