package org.apache.ranger.view;

import java.io.Serializable;
import java.util.List;

public class RangerUsersAndGroups implements Serializable {

    private List<String> users;
    private List<String> groups;
    private Boolean isAdmin = false;

    public RangerUsersAndGroups(){}

    public RangerUsersAndGroups(List<String> users, List<String> groups){
        this.users = users;
        this.groups = groups;
    }

    public RangerUsersAndGroups(List<String> users, List<String> groups, Boolean isAdmin){
        this.users = users;
        this.groups = groups;
        this.isAdmin = isAdmin;
    }

    public List<String> getUsers() {
        return users;
    }

    public void setUsers(List<String> users) {
        this.users = users;
    }

    public List<String> getGroups() {
        return groups;
    }

    public void setGroups(List<String> groups) {
        this.groups = groups;
    }

    public Boolean getAdmin() {
        return isAdmin;
    }

    public void setAdmin(Boolean admin) {
        isAdmin = admin;
    }

}
