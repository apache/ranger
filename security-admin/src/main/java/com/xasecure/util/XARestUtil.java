package com.xasecure.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.xasecure.common.XAConfigUtil;
import com.xasecure.common.MessageEnums;
import com.xasecure.common.RESTErrorUtil;
import com.xasecure.common.StringUtil;
import com.xasecure.entity.XXPortalUser;
import com.xasecure.view.VXMessage;
import com.xasecure.view.VXResponse;
import com.xasecure.view.VXPortalUser;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class XARestUtil {
	static final Logger logger = Logger.getLogger(XARestUtil.class);

	@Autowired
	StringUtil stringUtil;

	@Autowired
	RESTErrorUtil restErrorUtil;

	@Autowired
	XAConfigUtil configUtil;

	void splitUserRoleList(Collection<String> collection) {
		Collection<String> newCollection = new ArrayList<String>();
		for (String role : collection) {
			String roles[] = role.split(",");
			for (int i = 0; i < roles.length; i++) {
				String str = roles[i];
				newCollection.add(str);
			}
		}
		collection.clear();
		collection.addAll(newCollection);
	}

	/**
	 * This method cleans up the data provided by the user for update
	 * 
	 * @param userProfile
	 * @return
	 */
	public void validateVUserProfileForUpdate(XXPortalUser gjUser,
			VXPortalUser userProfile) {

		List<VXMessage> messageList = new ArrayList<VXMessage>();

		// Email Update is allowed.
		// if (userProfile.getEmailAddress() != null
		// && !userProfile.getEmailAddress().equalsIgnoreCase(
		// gjUser.getEmailAddress())) {
		// throw restErrorUtil.createRESTException(
		// "Email address can't be updated",
		// MessageEnums.DATA_NOT_UPDATABLE, null, "emailAddress",
		// userProfile.getEmailAddress());
		// }

		// Login Id can't be changed
		if (userProfile.getLoginId() != null
				&& !gjUser.getLoginId().equalsIgnoreCase(
						userProfile.getLoginId())) {
			throw restErrorUtil.createRESTException(
					"Username can't be updated",
					MessageEnums.DATA_NOT_UPDATABLE, null, "loginId",
					userProfile.getLoginId());
		}
		// }
		userProfile.setFirstName(restErrorUtil.validateStringForUpdate(
				userProfile.getFirstName(), gjUser.getFirstName(),
				StringUtil.VALIDATION_NAME, "Invalid first name",
				MessageEnums.INVALID_INPUT_DATA, null, "firstName"));

		userProfile.setFirstName(restErrorUtil.validateStringForUpdate(
				userProfile.getFirstName(), gjUser.getFirstName(),
				StringUtil.VALIDATION_NAME, "Invalid first name",
				MessageEnums.INVALID_INPUT_DATA, null, "firstName"));

		userProfile.setLastName(restErrorUtil.validateStringForUpdate(
				userProfile.getLastName(), gjUser.getLastName(),
				StringUtil.VALIDATION_NAME, "Invalid last name",
				MessageEnums.INVALID_INPUT_DATA, null, "lastName"));

		// firstName
		if (!stringUtil.isValidName(userProfile.getFirstName())) {
			logger.info("Invalid first name." + userProfile);
			messageList.add(MessageEnums.INVALID_INPUT_DATA.getMessage(null,
					"firstName"));
		}

		// lastName
		if (!stringUtil.isValidName(userProfile.getLastName())) {
			logger.info("Invalid last name." + userProfile);
			messageList.add(MessageEnums.INVALID_INPUT_DATA.getMessage(null,
					"lastName"));
		}

		// create the public screen name
		userProfile.setPublicScreenName(userProfile.getFirstName() + " "
				+ userProfile.getLastName());

		userProfile.setNotes(restErrorUtil.validateStringForUpdate(
				userProfile.getNotes(), gjUser.getNotes(),
				StringUtil.VALIDATION_NAME, "Invalid notes",
				MessageEnums.INVALID_INPUT_DATA, null, "notes"));

		// validate user roles
		if (userProfile.getUserRoleList() != null) {
			// First let's normalize it
			splitUserRoleList(userProfile.getUserRoleList());
			for (String userRole : userProfile.getUserRoleList()) {
				restErrorUtil.validateStringList(userRole,
						configUtil.getRoles(), "Invalid role", null,
						"userRoleList");
			}

		}
		if (messageList.size() > 0) {
			VXResponse gjResponse = new VXResponse();
			gjResponse.setStatusCode(VXResponse.STATUS_ERROR);
			gjResponse.setMsgDesc("Validation failure");
			gjResponse.setMessageList(messageList);
			logger.info("Validation Error in updateUser() userProfile="
					+ userProfile + ", error=" + gjResponse);
			throw restErrorUtil.createRESTException(gjResponse);
		}

	}

}