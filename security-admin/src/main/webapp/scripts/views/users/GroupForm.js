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

	var Backbone		= require('backbone');
	var XAEnums			= require('utils/XAEnums');
	
	require('backbone-forms');
	require('backbone-forms.templates');
	var GroupForm = Backbone.Form.extend(
	/** @lends GroupForm */
	{
		_viewName : 'GroupForm',

    	/**
		* intialize a new GroupForm Form View 
		* @constructs
		*/
		initialize: function(options) {
			console.log("initialized a GroupForm Form View");
			_.extend(this, _.pick(options,''));
    		Backbone.Form.prototype.initialize.call(this, options);

			this.bindEvents();
		},

		/** all events binding here */
		bindEvents : function(){
		},

		/** fields for the form
		*/
		fields: ['name', 'description'],
		schema :{},
		/** on render callback */
		render: function(options) {
			Backbone.Form.prototype.render.call(this, options);
			this.initializePlugins();
			if(!this.model.isNew()){
				if(!_.isUndefined(this.model.get('groupSource')) && this.model.get('groupSource') == XAEnums.GroupSource.XA_GROUP.value){
					this.fields.name.editor.$el.attr('disabled',true);
					this.fields.description.editor.$el.attr('disabled',true);
				}
			}
		},
		/** all post render plugin initialization */
		initializePlugins: function(){
		}
		
	});

	return GroupForm;
});
