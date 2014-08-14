/**
 *
 */
package com.xasecure.common;

/**
 *
 *
 */
public class SortField {
    public enum SORT_ORDER {
	ASC, DESC
    };

    String paramName;
    String fieldName;
    boolean isDefault = false;
    SORT_ORDER defaultOrder = SORT_ORDER.ASC;

    /**
     * @param string
     * @param string2
     */
    public SortField(String paramName, String fieldName) {
	this.paramName = paramName;
	this.fieldName = fieldName;
	isDefault = false;
    }

    /**
     * @param paramName
     * @param fieldName
     * @param isDefault
     */
    public SortField(String paramName, String fieldName, boolean isDefault,
	    SORT_ORDER defaultOrder) {
	this.paramName = paramName;
	this.fieldName = fieldName;
	this.isDefault = isDefault;
	this.defaultOrder = defaultOrder;
    }

    /**
     * @return the paramName
     */
    public String getParamName() {
	return paramName;
    }

    

    /**
     * @return the fieldName
     */
    public String getFieldName() {
	return fieldName;
    }

    

    /**
     * @return the isDefault
     */
    public boolean isDefault() {
	return isDefault;
    }

    

    /**
     * @return the defaultOrder
     */
    public SORT_ORDER getDefaultOrder() {
        return defaultOrder;
    }

    


}
