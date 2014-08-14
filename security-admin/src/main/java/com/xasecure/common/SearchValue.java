/**
 *
 */
package com.xasecure.common;

import java.util.List;

import org.apache.log4j.Logger;

/**
 * 
 *
 */
public class SearchValue {
    static final Logger logger = Logger.getLogger(SearchValue.class);

    SearchField searchField;
    Object value = null;
    List<?> valueList = null;
    boolean isNull = false;

   /**
     * @return the value
     */
    public Object getValue() {
	if (value != null) {
	    return value;
	}
	if (valueList.size() == 1) {
	    return valueList.get(0);
	}
	logger.error("getValue() called for null.", new Throwable());
	return value;
    }

    

    /**
     * @return the valueList
     */
    public List<?> getValueList() {
	return valueList;
    }

    /**
     * @return the searchField
     */
    public SearchField getSearchField() {
	return searchField;
    }

   


    public boolean isList() {
	return valueList != null && valueList.size() > 1;
    }

}
