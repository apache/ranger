/**
 *
 */
package com.xasecure.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class SearchCriteria {

	int startIndex = 0;
	int maxRows = Integer.MAX_VALUE;
	String sortBy = null;
	String sortType = null;
	boolean getCount = true;
	Number ownerId = null;
	boolean familyOnly = false;
	boolean getChildren = false;
	boolean isDistinct = false;
	HashMap<String, Object> paramList = new HashMap<String, Object>();
	Set<String> nullParamList = new HashSet<String>();
	Set<String> notNullParamList = new HashSet<String>();

	List<SearchGroup> searchGroups = new ArrayList<SearchGroup>();

	/**
	 * @return the startIndex
	 */
	public int getStartIndex() {
		return startIndex;
	}

	/**
	 * @param startIndex
	 *            the startIndex to set
	 */
	public void setStartIndex(int startIndex) {
		this.startIndex = startIndex;
	}

	/**
	 * @return the maxRows
	 */
	public int getMaxRows() {
		return maxRows;
	}

	/**
	 * @param maxRows
	 *            the maxRows to set
	 */
	public void setMaxRows(int maxRows) {
		this.maxRows = maxRows;
	}

	/**
	 * @return the sortBy
	 */
	public String getSortBy() {
		return sortBy;
	}

	/**
	 * @param sortBy
	 *            the sortBy to set
	 */
	public void setSortBy(String sortBy) {
		this.sortBy = sortBy;
	}

	/**
	 * @return the sortType
	 */
	public String getSortType() {
		return sortType;
	}

	/**
	 * @param sortType
	 *            the sortType to set
	 */
	public void setSortType(String sortType) {
		this.sortType = sortType;
	}

	public boolean isGetCount() {
		return getCount;
	}

	public void setGetCount(boolean getCount) {
		this.getCount = getCount;
	}

	public Number getOwnerId() {
		return ownerId;
	}

	public void setOwnerId(Number ownerId) {
		this.ownerId = ownerId;
	}

	public boolean isGetChildren() {
		return getChildren;
	}

	public void setGetChildren(boolean getChildren) {
		this.getChildren = getChildren;
	}

	/**
	 * @return the paramList
	 */
	public HashMap<String, Object> getParamList() {
		return paramList;
	}

	/**
	 * @param string
	 * @param caId
	 */
	public void addParam(String name, Object value) {
		paramList.put(name, value);
	}

	public Object getParamValue(String name) {
		return paramList.get(name);
	}

	/**
	 * @return the nullParamList
	 */
	public Set<String> getNullParamList() {
		return nullParamList;
	}

	/**
	 * @return the notNullParamList
	 */
	public Set<String> getNotNullParamList() {
		return notNullParamList;
	}

	/**
	 * @return the searchGroups
	 */
	public List<SearchGroup> getSearchGroups() {
		return searchGroups;
	}

	/**
	 * @return the isDistinct
	 */
	public boolean isDistinct() {
		return isDistinct;
	}

	/**
	 * @param isDistinct
	 *            the isDistinct to set
	 */
	public void setDistinct(boolean isDistinct) {
		this.isDistinct = isDistinct;
	}

}
