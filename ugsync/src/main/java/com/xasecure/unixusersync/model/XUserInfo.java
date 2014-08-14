package com.xasecure.unixusersync.model;

import java.util.ArrayList;
import java.util.List;

public class XUserInfo {
	private String id ;
	private String name ;
	private String 	description ;
	
	private List<String>  groups = new ArrayList<String>() ;
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	
	public List<String> getGroups() {
		return groups;
	}
	
}
