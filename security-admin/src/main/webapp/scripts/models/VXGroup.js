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

	var VXGroupBase		= require('model_bases/VXGroupBase');
	var localization	= require('utils/XALangSupport');
	
	var VXGroup = VXGroupBase.extend(
	/** @lends VXGroup.prototype */
	{
		/**
		 * VXGroup initialize method
		 * @augments XABaseModel
		 * @constructs
		 */
		initialize: function() {
			this.modelName = 'VXGroup';
			this.bindErrorEvents();
			
		},
		/**
		 * @function schema
		 * This method is meant to be used by UI,
		 * by default we will remove the unrequired attributes from serverSchema
		 */

		schema : function(){
			var attrs = _.omit(this.serverSchema, 'id', 'createDate', 'updateDate', "version",
					"createDate", "updateDate", "displayOption",
					"permList", "forUserId", "status", "priGrpId",
					"priAcctId", "updatedBy",
					"isSystem","credStoreId","description","groupType");
			
			return _.extend(attrs,{
				name : {
					type		: 'Text',
					title		: localization.tt("lbl.groupName") +' *',
					validators  : ['required',{type:'regexp',regexp:/^[a-zA-Z][a-zA-Z0-9_'&-]*[A-Za-z0-9]$/i,message :'Please enter valid name.'}],
					editorAttrs 	:{ 'maxlength': 32},
				},
				description : {
					type		: 'TextArea',
					title		: localization.tt("lbl.description")
				}
			});	
		},

		/*links : {
			detail: {
				href: 'javascript:void(0)',
				label : this.toString()
			},
			list: {
				href: 'javascript:void(0)',
				label : this.toString()
			},
		},*/
		
		/** This models toString() */
		toString : function(){
			return /*this.get('name')*/;
		}

	}, {
		// static class members
	});

    return VXGroup;
	
});


