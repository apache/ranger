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

	var VXAuditRecordBase	= require('model_bases/VXAuditRecordBase');

	var VXAuditRecord = VXAuditRecordBase.extend(
	/** @lends VXAuditRecord.prototype */
	{
		/**
		 * @function schema
		 * This method is meant to be used by UI,
		 * by default we will remove the unrequired attributes from serverSchema
		 */

		schemaBase : function(){
			var attrs = _.omit(this.serverSchema, 'id', 'createDate', 'updateDate', "version",
					"createDate", "updateDate", "displayOption",
					"permList", "forUserId", "status", "priGrpId",
					"updatedBy","isSystem");

			_.each(attrs, function(o){
				o.type = 'Hidden';
			});

			// Overwrite your schema definition here
			return _.extend(attrs,{
				/*name : {
					type		: 'Text',
					title		: 'Folder Name *',
					validators	: ['required'],
				},*/

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

    return VXAuditRecord;
	
});


