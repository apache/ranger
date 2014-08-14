package com.xasecure.common.view;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlRootElement;

import com.xasecure.common.AppConstants;

import com.xasecure.common.XACommonEnums;
import com.xasecure.common.view.ViewBaseBean;

@XmlRootElement
public class VTrxLogAttr extends ViewBaseBean implements Serializable{
	private static final long serialVersionUID = 1L;
	
	protected String attribName;
	protected String attribUserFriendlyName;
	protected boolean isEnum;
	
	public VTrxLogAttr(){}

	public VTrxLogAttr(String attribName, String attribUserFriendlyName,
			boolean isEnum) {
		super();
		this.attribName = attribName;
		this.attribUserFriendlyName = attribUserFriendlyName;
		this.isEnum = isEnum;
	}

	


	/**
	 * @return the attribUserFriendlyName
	 */
	public String getAttribUserFriendlyName() {
		return attribUserFriendlyName;
	}


	/**
	 * @return the isEnum
	 */
	public boolean isEnum() {
		return isEnum;
	}

	
	
	@Override
	public int getMyClassType( ) {
	    return AppConstants.CLASS_TYPE_XA_TRANSACTION_LOG_ATTRIBUTE;
	}

	@Override
	public String toString(){
		String str = "VTrxLogAttr={";
		str += super.toString();
		str += "attribName={" + attribName + "} ";
		str += "attribUserFriendlyName={" + attribUserFriendlyName + "} ";
		str += "isEnum={" + isEnum + "} ";
		str += "}";
		return str;
	}
}
