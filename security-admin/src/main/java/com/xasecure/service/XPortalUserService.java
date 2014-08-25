package com.xasecure.service;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.xasecure.common.AppConstants;
import com.xasecure.common.StringUtil;
import com.xasecure.common.view.VTrxLogAttr;
import com.xasecure.entity.*;
import com.xasecure.util.XAEnumUtil;
import com.xasecure.view.*;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

@Service
@Scope("singleton")
public class XPortalUserService extends
		XPortalUserServiceBase<XXPortalUser, VXPortalUser> {

	@Autowired
	XAEnumUtil xaEnumUtil;

	@Autowired
	StringUtil stringUtil;

	static HashMap<String, VTrxLogAttr> trxLogAttrs = new HashMap<String, VTrxLogAttr>();
	static {
		trxLogAttrs.put("loginId",
				new VTrxLogAttr("loginId", "Login ID", false));
		trxLogAttrs.put("status", new VTrxLogAttr("status",
				"Activation Status", false));
		trxLogAttrs.put("firstName", new VTrxLogAttr("firstName", "First Name",
				false));
		trxLogAttrs.put("lastName", new VTrxLogAttr("lastName", "Last Name",
				false));
		trxLogAttrs.put("emailAddress", new VTrxLogAttr("emailAddress",
				"Email Address", false));
		trxLogAttrs.put("publicScreenName", new VTrxLogAttr("publicScreenName",
				"Public Screen Name", false));
	}

	@Override
	protected void validateForCreate(VXPortalUser vObj) {
		// TODO Auto-generated method stub

	}

	@Override
	protected void validateForUpdate(VXPortalUser vObj, XXPortalUser mObj) {
		// TODO Auto-generated method stub

	}

	public List<XXTrxLog> getTransactionLog(VXPortalUser vUser, String action) {
		return getTransactionLog(vUser, null, action);
	}

	public List<XXTrxLog> getTransactionLog(VXPortalUser vObj,
			XXPortalUser xObj, String action) {
		if (vObj == null
				&& (action == null || !action.equalsIgnoreCase("update"))) {
			return null;
		}

		List<XXTrxLog> trxLogList = new ArrayList<XXTrxLog>();
		Field[] fields = vObj.getClass().getDeclaredFields();

		try {
			Field nameField = vObj.getClass().getDeclaredField("loginId");
			nameField.setAccessible(true);
			String objectName = "" + nameField.get(vObj);

			for (Field field : fields) {
				field.setAccessible(true);
				String fieldName = field.getName();
				if (!trxLogAttrs.containsKey(fieldName)) {
					continue;
				}

				VTrxLogAttr vTrxLogAttr = trxLogAttrs.get(fieldName);

				XXTrxLog xTrxLog = new XXTrxLog();
				xTrxLog.setAttributeName(vTrxLogAttr
						.getAttribUserFriendlyName());

				String value = null;
				boolean isEnum = vTrxLogAttr.isEnum();
				if (isEnum) {
					String enumName = XXAsset.getEnumName(fieldName);
					int enumValue = field.get(vObj) == null ? 0 : Integer
							.parseInt("" + field.get(vObj));
					value = xaEnumUtil.getLabel(enumName, enumValue);
				} else {
					value = "" + field.get(vObj);
				}

				if (action.equalsIgnoreCase("create")) {
					if (stringUtil.isEmpty(value)) {
						continue;
					}
					xTrxLog.setNewValue(value);
				} else if (action.equalsIgnoreCase("delete")) {
					xTrxLog.setPreviousValue(value);
				} else if (action.equalsIgnoreCase("update")) {
					String oldValue = null;
					Field[] xFields = xObj.getClass().getDeclaredFields();

					for (Field xField : xFields) {
						xField.setAccessible(true);
						String xFieldName = xField.getName();

						if (fieldName.equalsIgnoreCase(xFieldName)) {
							if (isEnum) {
								String enumName = XXAsset
										.getEnumName(xFieldName);
								int enumValue = xField.get(xObj) == null ? 0
										: Integer.parseInt(""
												+ xField.get(xObj));
								oldValue = xaEnumUtil.getLabel(enumName,
										enumValue);
							} else {
								oldValue = xField.get(xObj) + "";
							}
							break;
						}
					}
					if (fieldName.equalsIgnoreCase("emailAddress")) {
						if (!stringUtil.validateEmail(oldValue)) {
							oldValue = "";
						}
						if (!stringUtil.validateEmail(value)) {
							value = "";
						}
					}
					if (value.equalsIgnoreCase(oldValue)) {
						continue;
					}
					xTrxLog.setPreviousValue(oldValue);
					xTrxLog.setNewValue(value);
				}

				xTrxLog.setAction(action);
				xTrxLog.setObjectClassType(AppConstants.CLASS_TYPE_USER_PROFILE);
				xTrxLog.setObjectId(vObj.getId());
				xTrxLog.setObjectName(objectName);
				trxLogList.add(xTrxLog);
			}
		} catch (IllegalArgumentException e) {
			logger.info(
					"Caught IllegalArgumentException while"
							+ " getting Transaction log for user : "
							+ vObj.getLoginId(), e);
		} catch (NoSuchFieldException e) {
			logger.info(
					"Caught NoSuchFieldException while"
							+ " getting Transaction log for user : "
							+ vObj.getLoginId(), e);
		} catch (SecurityException e) {
			logger.info(
					"Caught SecurityException while"
							+ " getting Transaction log for user : "
							+ vObj.getLoginId(), e);
		} catch (IllegalAccessException e) {
			logger.info(
					"Caught IllegalAccessException while"
							+ " getting Transaction log for user : "
							+ vObj.getLoginId(), e);
		}
		return trxLogList;
	}

}
