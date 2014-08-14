/*
 * Copyright (c) 2014 XASecure
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of
 * XASecure. ("Confidential Information").  You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with XASecure.
 */

// Manages the user session
define(function(require){
	'use strict';

	var VXPortalUser		= require('models/VXPortalUser');

	// Private properties
	var vXPortalUser = null;
	var vSessionSettings = null;

	// Public methods
	var SessionMgr = {};

	/**
	 * Gets the user profile for the given session
	 * 
	 * @returns VXPortalUser
	 */

	SessionMgr.getUserProfile = function() {
		if ( vXPortalUser){
			return vXPortalUser;
		}

		vXPortalUser = new VXPortalUser();
		vXPortalUser.getUserProfile({async : false,cache:false}).done(function(data){
			vXPortalUser.set(data);
		});
		return vXPortalUser;
	};


	SessionMgr.getLoginId = function() {
		if (vXPortalUser) {
			return vXPortalUser.get('loginId');
		}
	};

	SessionMgr.userInRole = function(role) {
		var vXPortalUser = SessionMgr.getUserProfile();
		var userRoles = vXPortalUser.get('userRoleList');
		if (!userRoles || !role) {
			return false;
		}
		if (userRoles.constructor != Array) {
			userRoles = [ userRoles ];
		}

		return (userRoles.indexOf(role) > -1);
	};

	SessionMgr.getUserRoles = function() {
		var vXPortalUser = SessionMgr.getUserProfile();
		var userRoles = vXPortalUser.get('userRoleList');
		if (!userRoles) {
			return [];
		}
		if (userRoles.constructor != Array) {
			userRoles = [ userRoles ];
		}

		return userRoles;
	};

	SessionMgr.getSetting = function(key) {
		if (!vSessionSettings) {
			var msResponse = GeneralMgr.getSessionSettings();
			if (msResponse.isSuccess()) {
				vSessionSettings = msResponse.response;
			}
		}
		var value = null;
		if (vSessionSettings && key) {
			vSessionSettings.each(function(vNameValue) {
				if (vNameValue.get('name') == key) {
					value = vNameValue.get('value');
				}
			});
		}
		return value;
	};

	SessionMgr.resetSession = function() {
		vXPortalUser = null;
		vSessionSettings = null;
		MSCacheMgr.resetAll();
	};

	/**
	 * Logs out the user and resets all session variables
	 */
	SessionMgr.logout = function(reDirectUser) {
		SessionMgr.resetSession();
		MSCacheMgr.resetAll();
		if (reDirectUser) {
			// This will ask the browser to redirect
			window.location.replace("logout.html");
		} else {
			// We will do an implicit logout
			$.ajax({
				url : 'logout.html',
				type : 'GET',
				async : false
			});
		}
	};

	SessionMgr.isSystemAdmin = function(){
		return this.userInRole('ROLE_SYS_ADMIN') ? true : false;
	};
	
	SessionMgr.isUser = function(){
		var roles = this.getRoleInUserSchool();
		return  $.inArray('ROLE_USER',roles) != -1  ? true  : false ;
	};
	return SessionMgr;
});	
