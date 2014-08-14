package com.xasecure.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.xasecure.common.XAConstants;
import com.xasecure.common.ContextUtil;
import com.xasecure.common.XAConfigUtil;
import com.xasecure.common.MessageEnums;
import com.xasecure.common.StringUtil;
import com.xasecure.common.UserSessionBase;
import com.xasecure.entity.XXPortalUser;
import com.xasecure.entity.XXPortalUserRole;
import com.xasecure.view.VXMessage;
import com.xasecure.view.VXResponse;
import com.xasecure.view.VXPortalUser;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

@Service
@Scope("singleton")
public class UserService extends UserServiceBase<XXPortalUser, VXPortalUser> {
	static Logger logger = Logger.getLogger(UserService.class);

	public static final String NAME = "User";

	@Autowired
	XAConfigUtil configUtil;

	private static UserService instance = null;

	public UserService() {
		super();
		instance = this;
	}

	public static UserService getInstance() {
		if (instance == null) {
			logger.error("Instance is null", new Throwable());
		}
		return instance;
	}

	@Override
	protected void validateForCreate(VXPortalUser userProfile) {
		List<VXMessage> messageList = new ArrayList<VXMessage>();
		if (stringUtil.isEmpty(userProfile.getEmailAddress())) {
			logger.info("Empty Email Address." + userProfile);
			messageList.add(MessageEnums.NO_INPUT_DATA.getMessage(null,
					"emailAddress"));
		}

		if (stringUtil.isEmpty(userProfile.getFirstName())) {
			logger.info("Empty firstName." + userProfile);
			messageList.add(MessageEnums.NO_INPUT_DATA.getMessage(null,
					"firstName"));
		}
		if (stringUtil.isEmpty(userProfile.getLastName())) {
			logger.info("Empty lastName." + userProfile);
			messageList.add(MessageEnums.NO_INPUT_DATA.getMessage(null,
					"lastName"));
		}
		// firstName
		if (!stringUtil.isValidName(userProfile.getFirstName())) {
			logger.info("Invalid first name." + userProfile);
			messageList.add(MessageEnums.INVALID_INPUT_DATA.getMessage(null,
					"firstName"));
		}
		userProfile.setFirstName(stringUtil.toCamelCaseAllWords(userProfile
				.getFirstName()));

		// lastName
		if (!stringUtil.isValidName(userProfile.getLastName())) {
			logger.info("Invalid last name." + userProfile);
			messageList.add(MessageEnums.INVALID_INPUT_DATA.getMessage(null,
					"lastName"));
		}
		userProfile.setLastName(stringUtil.toCamelCaseAllWords(userProfile
				.getLastName()));

		if (!stringUtil.validateEmail(userProfile.getEmailAddress())) {
			logger.info("Invalid email address." + userProfile);
			messageList.add(MessageEnums.INVALID_INPUT_DATA.getMessage(null,
					"emailAddress"));

		}

		// Normalize email. Make it lower case
		userProfile.setEmailAddress(stringUtil.normalizeEmail(userProfile
				.getEmailAddress()));

		// loginId
		userProfile.setLoginId(userProfile.getEmailAddress());

		// password
		if (!stringUtil.validatePassword(
				userProfile.getPassword(),
				new String[] { userProfile.getFirstName(),
						userProfile.getLastName() })) {
			logger.info("Invalid password." + userProfile);
			messageList.add(MessageEnums.INVALID_INPUT_DATA.getMessage(null,
					"password"));
		}

		// firstName
		if (!stringUtil.validateString(StringUtil.VALIDATION_NAME,
				userProfile.getFirstName())) {
			logger.info("Invalid first name." + userProfile);
			messageList.add(MessageEnums.INVALID_INPUT_DATA.getMessage(null,
					"firstName"));
		}

		// lastName
		if (!stringUtil.validateString(StringUtil.VALIDATION_NAME,
				userProfile.getLastName())) {
			logger.info("Invalid last name." + userProfile);
			messageList.add(MessageEnums.INVALID_INPUT_DATA.getMessage(null,
					"lastName"));
		}

		// create the public screen name
		userProfile.setPublicScreenName(userProfile.getFirstName() + " "
				+ userProfile.getLastName());

		if (messageList.size() > 0) {
			VXResponse gjResponse = new VXResponse();
			gjResponse.setStatusCode(VXResponse.STATUS_ERROR);
			gjResponse.setMsgDesc("Validation failure");
			gjResponse.setMessageList(messageList);
			logger.info("Validation Error in createUser() userProfile="
					+ userProfile + ", error=" + gjResponse);
			throw restErrorUtil.createRESTException(gjResponse);
		}
	}

	@Override
	protected void validateForUpdate(VXPortalUser userProfile, XXPortalUser xXPortalUser) {
		List<VXMessage> messageList = new ArrayList<VXMessage>();

		if (userProfile.getEmailAddress() != null
				&& !userProfile.getEmailAddress().equalsIgnoreCase(
						xXPortalUser.getEmailAddress())) {
			throw restErrorUtil.createRESTException("serverMsg.userEmail",
					MessageEnums.DATA_NOT_UPDATABLE, null, "emailAddress",
					userProfile.getEmailAddress());
		}

		// Login Id can't be changed
		if (userProfile.getLoginId() != null
				&& !xXPortalUser.getLoginId().equalsIgnoreCase(
						userProfile.getLoginId())) {
			throw restErrorUtil.createRESTException("serverMsg.userUserName",
					MessageEnums.DATA_NOT_UPDATABLE, null, "loginId",
					userProfile.getLoginId());
		}
		// }

		userProfile.setFirstName(restErrorUtil.validateStringForUpdate(
				userProfile.getFirstName(), xXPortalUser.getFirstName(),
				StringUtil.VALIDATION_NAME, "serverMsg.userFirstName",
				MessageEnums.INVALID_INPUT_DATA, null, "firstName"));

		userProfile.setFirstName(restErrorUtil.validateStringForUpdate(
				userProfile.getFirstName(), xXPortalUser.getFirstName(),
				StringUtil.VALIDATION_NAME, "serverMsg.userFirstName",
				MessageEnums.INVALID_INPUT_DATA, null, "firstName"));

		userProfile.setLastName(restErrorUtil.validateStringForUpdate(
				userProfile.getLastName(), xXPortalUser.getLastName(),
				StringUtil.VALIDATION_NAME, "serverMsg.userLastName",
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

		userProfile.setNotes(restErrorUtil.validateStringForUpdate(
				userProfile.getNotes(), xXPortalUser.getNotes(),
				StringUtil.VALIDATION_NAME, "serverMsg.userNotes",
				MessageEnums.INVALID_INPUT_DATA, null, "notes"));

		// validate status
		restErrorUtil.validateMinMax(userProfile.getStatus(), 0,
				XAConstants.ActivationStatus_MAX, "Invalid status", null,
				"status");

		// validate user roles
		if (userProfile.getUserRoleList() != null) {
			// First let's normalize it
			splitUserRoleList(userProfile.getUserRoleList());
			for (String userRole : userProfile.getUserRoleList()) {
				restErrorUtil.validateStringList(userRole,
						configUtil.getRoles(), "serverMsg.userRole", null,
						"userRoleList");
			}

		}

		// TODO: Need to see whether user can set user as internal

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

	@Override
	protected XXPortalUser mapViewToEntityBean(VXPortalUser userProfile, XXPortalUser mObj,
			int OPERATION_CONTEXT) {
		mObj.setEmailAddress(userProfile.getEmailAddress());
		mObj.setFirstName(userProfile.getFirstName());
		mObj.setLastName(userProfile.getLastName());
		mObj.setLoginId(userProfile.getLoginId());
		mObj.setPassword(userProfile.getPassword());
		mObj.setPublicScreenName(bizUtil.generatePublicName(userProfile, null));
		mObj.setUserSource(userProfile.getUserSource());
		return mObj;

	}

	@Override
	protected VXPortalUser mapEntityToViewBean(VXPortalUser userProfile,
			XXPortalUser user) {
		userProfile.setId(user.getId());
		userProfile.setLoginId(user.getLoginId());
		userProfile.setFirstName(user.getFirstName());
		userProfile.setLastName(user.getLastName());
		userProfile.setPublicScreenName(user.getPublicScreenName());
		userProfile.setStatus(user.getStatus());
		userProfile.setUserRoleList(new ArrayList<String>());
		String emailAddress = user.getEmailAddress();
		if (emailAddress != null && stringUtil.validateEmail(emailAddress)) {
			userProfile.setEmailAddress(user.getEmailAddress());
		}

		UserSessionBase sess = ContextUtil.getCurrentUserSession();
		if (sess != null) {
			userProfile.setUserSource(sess.getAuthProvider());
		}

		List<XXPortalUserRole> gjUserRoleList = daoMgr.getXXPortalUserRole().findByParentId(
				user.getId());

		for (XXPortalUserRole gjUserRole : gjUserRoleList) {
			userProfile.getUserRoleList().add(gjUserRole.getUserRole());
		}
		return userProfile;
	}

	// TODO: Need to remove this ASAP
	public void gjUserToUserProfile(XXPortalUser user, VXPortalUser userProfile) {
		userProfile.setId(user.getId());
		userProfile.setLoginId(user.getLoginId());
		userProfile.setFirstName(user.getFirstName());
		userProfile.setLastName(user.getLastName());
		userProfile.setPublicScreenName(user.getPublicScreenName());
		userProfile.setStatus(user.getStatus());
		userProfile.setUserRoleList(new ArrayList<String>());
		UserSessionBase sess = ContextUtil.getCurrentUserSession();

		String emailAddress = user.getEmailAddress();
		if (emailAddress != null && stringUtil.validateEmail(emailAddress)) {
			userProfile.setEmailAddress(user.getEmailAddress());
		}

		if (sess != null) {
			userProfile.setUserSource(sess.getAuthProvider());
		}

		List<XXPortalUserRole> gjUserRoleList = daoMgr.getXXPortalUserRole().findByParentId(
				user.getId());

		for (XXPortalUserRole gjUserRole : gjUserRoleList) {
			userProfile.getUserRoleList().add(gjUserRole.getUserRole());
		}
	}

}
