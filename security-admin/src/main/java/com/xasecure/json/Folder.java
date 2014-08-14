package com.xasecure.json;

import java.util.List;

public class Folder {

	String name;
	List<Folder> folders;
	
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public List<Folder> getFolders() {
		return folders;
	}
	public void setFolders(List<Folder> folders) {
		this.folders = folders;
	}
	
}
