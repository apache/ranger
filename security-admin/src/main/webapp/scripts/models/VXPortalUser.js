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

define(function(require){
	'use strict';	

	var VXPortalUserBase	= require('model_bases/VXPortalUserBase');
	var XAEnums			= require('utils/XAEnums');
	var XAUtils			= require('utils/XAUtils');
	var localization		= require('utils/XALangSupport');
	
	var VXPortalUser = VXPortalUserBase.extend(
	/** @lends VXPortalUser.prototype */
	{
		/**
		 * @function schema
		 * This method is meant to be used by UI,
		 * by default we will remove the unrequired attributes from serverSchema
		 */

		schema : function(){
			var attrs = _.omit(this.serverSchema, 'id', 'createDate', 'updateDate', "version",
					"createDate", "updateDate", "displayOption",
					"permList", "forUserId", "status", "priGrpId",
					 "updatedBy","isSystem");

			_.each(attrs, function(o){
				o.type = 'Hidden';
			});

			// Overwrite your schema definition here
			return _.extend(attrs,{
				firstName : {
					type		: 'Text',
					title		: localization.tt("lbl.firstName")+' *',
					validators  : ['required',{type:'regexp',regexp:/^[a-z][a-z0-9]+$/i,message :'Please enter valid name'}],
					editorAttrs : { 'placeholder' : localization.tt("lbl.firstName")}
					
				},
				lastName : {
					type		: 'Text',
					title		: localization.tt("lbl.lastName")+' *',
					validators  : ['required',{type:'regexp',regexp:/^[a-z][a-z0-9]+$/i,message :'Please enter valid name'}],
					editorAttrs : { 'placeholder' : localization.tt("lbl.lastName")}
				},
				emailAddress : {
					type		: 'Text',
					title		: localization.tt("lbl.emailAddress"),
					validators  : ['email'],
					editorAttrs : { 'placeholder' : localization.tt("lbl.emailAddress")}//'disabled' : true}
					
				},
				oldPassword : {
					type		: 'Password',
					title		: localization.tt("lbl.oldPassword")+' *',
				//	validators  : ['required'],
					fieldAttrs : {style : 'display:none;'},
					editorAttrs : { 'placeholder' : localization.tt("lbl.oldPassword"),'onpaste':'return false;','oncopy':'return false;'}
					
				},
				newPassword : {
					type		: 'Password',
					title		: localization.tt("lbl.newPassword")+' *',
				//	validators  : ['required'],
					fieldAttrs : {style : 'display:none;'},
					editorAttrs : { 'placeholder' : localization.tt("lbl.newPassword"),'onpaste':'return false;','oncopy':'return false;'}
					
				},
				reEnterPassword : {
					type		: 'Password',
					title		: localization.tt("lbl.reEnterPassword")+' *',
				//	validators  : ['required'],
					fieldAttrs : {style : 'display:none;'},
					editorAttrs : { 'placeholder' : localization.tt("lbl.reEnterPassword"),'onpaste':'return false;','oncopy':'return false;'}
					
				},
				userRoleList : {
					type : 'Select',
					options : function(callback, editor){
						var userTypes = _.filter(XAEnums.UserRoles,function(m){return m.label != 'Unknown'});
						var nvPairs = XAUtils.enumToSelectPairs(userTypes);
						callback(nvPairs);
					},
					title : localization.tt('lbl.selectRole')+' *',
					editorAttrs : {disabled:'disabled'},
				}
			});
		},
		/** This models toString() */
		toString : function(){
			return /*this.get('name')*/;
		}

	}, {
		// static class members
	});

    return VXPortalUser;
	
});


