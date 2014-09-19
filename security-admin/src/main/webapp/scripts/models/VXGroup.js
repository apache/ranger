/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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


