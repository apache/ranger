package com.xasecure.admin.client.datatype;


import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;


@JsonAutoDetect(getterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE, fieldVisibility = Visibility.ANY)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class GrantRevokeData implements java.io.Serializable {
	private static final long serialVersionUID = 1L;

	private String              grantor;
	private String              repositoryName;
	private String              repositoryType;
	private String              databases;
	private String              tables;
	private String              columns;
	private String              columnFamilies;
	private List<UserPermList>  userPermList  = new ArrayList<UserPermList>();
	private List<GroupPermList> groupPermList = new ArrayList<GroupPermList>();


	public GrantRevokeData() {
	}
	

	public String getGrantor() {
		return grantor;
	}

	public void setGrantor(String grantor) {
		this.grantor = grantor;
	}

	public String getRepositoryName() {
		return repositoryName;
	}

	public void setRepositoryName(String repositoryName) {
		this.repositoryName = repositoryName;
	}

	public String getRepositoryType() {
		return repositoryType;
	}

	public void setRepositoryType(String repositoryType) {
		this.repositoryType = repositoryType;
	}

	public String getDatabases() {
		return databases;
	}

	public void setDatabases(String databases) {
		this.databases = databases;
	}

	public String getTables() {
		return tables;
	}

	public void setTables(String tables) {
		this.tables = tables;
	}

	public String getColumns() {
		return columns;
	}

	public void setColumns(String columns) {
		this.columns = columns;
	}

	public String getColumnFamilies() {
		return columnFamilies;
	}

	public void setColumnFamilies(String columnFamilies) {
		this.columnFamilies = columnFamilies;
	}

	public List<UserPermList> getUserPermList() {
		return userPermList;
	}

	public void setUserPermList(List<UserPermList> userPermList) {
		this.userPermList = userPermList;
	}

	public List<GroupPermList> getGroupPermList() {
		return groupPermList;
	}

	public void setGroupPermList(List<GroupPermList> groupPermList) {
		this.groupPermList = groupPermList;
	}


	public void setHiveData(String              grantor,
							String              repositoryName,
							String              databases,
							String              tables,
							String              columns,
							List<UserPermList>  userPermList,
							List<GroupPermList> groupPermList) {
		this.grantor         = grantor;
		this.repositoryName = repositoryName;
		this.repositoryType = "hive";
		this.databases      = databases;
		this.tables         = tables;
		this.columns        = columns;

		for(UserPermList userPerm : userPermList) {
			this.userPermList.add(userPerm);
		}

		for(GroupPermList groupPerm : groupPermList) {
			this.groupPermList.add(groupPerm);
		}
	}

	public void setHBaseData(String              grantor,
							 String              repositoryName,
							 String              tables,
							 String              columns,
							 String              columnFamilies,
							 List<UserPermList>  userPermList,
							 List<GroupPermList> groupPermList) {
		this.grantor         = grantor;
		this.repositoryName = repositoryName;
		this.repositoryType = "hbase";
		this.tables         = tables;
		this.columns        = columns;
		this.columnFamilies = columnFamilies;

		for(UserPermList userPerm : userPermList) {
			this.userPermList.add(userPerm);
		}

		for(GroupPermList groupPerm : groupPermList) {
			this.groupPermList.add(groupPerm);
		}
	}
	
	public String toJson() {
		try {
			ObjectMapper om = new ObjectMapper();

			return om.writeValueAsString(this);
		} catch (JsonGenerationException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return "";
	}

	@Override
	public String toString() {
		return toJson();
	}


	@JsonAutoDetect(getterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE, fieldVisibility = Visibility.ANY)
	@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
	@JsonIgnoreProperties(ignoreUnknown = true)
	public static class UserPermList {
		private List<String> userList = new ArrayList<String>();
		private List<String> permList = new ArrayList<String>();

		public UserPermList(String user, String perm) {
			addUser(user);
			addPerm(perm);
		}

		public UserPermList(List<String> userList, List<String> permList) {
			for(String user : userList) {
				addUser(user);
			}

			for(String perm : permList) {
				addPerm(perm);
			}
		}

		public List<String> getUserList() {
			return userList;
		}

		public List<String> getPermList() {
			return permList;
		}

		public void addUser(String user) {
			userList.add(user);
		}

		public void addPerm(String perm) {
			permList.add(perm);
		}

		public String toJson() {
			try {
				ObjectMapper om = new ObjectMapper();

				return om.writeValueAsString(this);
			} catch (JsonGenerationException e) {
				e.printStackTrace();
			} catch (JsonMappingException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			return "";
		}

		@Override
		public String toString() {
			return toJson();
		}
	}
	
	@JsonAutoDetect(getterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE, fieldVisibility = Visibility.ANY)
	@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
	@JsonIgnoreProperties(ignoreUnknown = true)
	public static class GroupPermList {
		List<String> groupList = new ArrayList<String>();
		List<String> permList  = new ArrayList<String>();

		public GroupPermList(String group, String perm) {
			addGroup(group);
			addPerm(perm);
		}

		public GroupPermList(List<String> groupList, List<String> permList) {
			for(String group : groupList) {
				addGroup(group);
			}

			for(String perm : permList) {
				addPerm(perm);
			}
		}

		public List<String> getGroupList() {
			return groupList;
		}

		public List<String> getPermList() {
			return permList;
		}

		public void addGroup(String group) {
			groupList.add(group);
		}

		public void addPerm(String perm) {
			permList.add(perm);
		}

		public String toJson() {
			try {
				ObjectMapper om = new ObjectMapper();

				return om.writeValueAsString(this);
			} catch (JsonGenerationException e) {
				e.printStackTrace();
			} catch (JsonMappingException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			return "";
		}

		@Override
		public String toString() {
			return toJson();
		}
	}
	
	public static void main(String[] args) {
		GrantRevokeData grData = new GrantRevokeData();
		
		System.out.println(grData.toString());
	}
}
