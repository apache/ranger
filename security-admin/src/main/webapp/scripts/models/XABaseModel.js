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

/**
 *
 * Base Model file from which all models will extend/derive.
 */

define(function(require){
	'use strict';

	var Backbone = require('backbone');
	var XAUtils	= require('utils/XAUtils');
	
	var XABaseModel = Backbone.Model.extend(
	/** @lends XABaseModel.prototype */
	{
		/**
		 * XABaseModel's initialize function
		 * @augments Backbone.Model
		 * @constructs
		 */
		initialize : function() {
			
		},
		bindErrorEvents :function(){
			this.bind("error", XAUtils.defaultErrorHandler);
		},
		/**
		 * toString for a model. Every model should implement this function.
		 */
		toString : function() {
			throw new Error('ERROR: toString() not defined for ' + this.modelName);
		},

		/**
		 * Silent'ly set the attributes. ( do not trigger events )
		 */
		silent_set: function(attrs) {
			return this.set(attrs, {
				silent: true
			});
		},
		parse: function(response){
			for(var key in this.modelRel)
				{
					var embeddedClass = this.modelRel[key];
					var embeddedData = response[key];
					response[key] = new embeddedClass(embeddedData);
				}
				return response;
		}

	}, {
		nestCollection : function(model, attributeName, nestedCollection) {
			//setup nested references
			for (var i = 0; i < nestedCollection.length; i++) {
				model.attributes[attributeName][i] = nestedCollection.at(i).attributes;
			}
			//create empty arrays if none

			nestedCollection.bind('add', function (initiative) {
				if (!model.get(attributeName)) {
					model.attributes[attributeName] = [];
				}
				model.get(attributeName).push(initiative.attributes);
			});

			nestedCollection.bind('remove', function (initiative) {
				var updateObj = {};
				updateObj[attributeName] = _.without(model.get(attributeName), initiative.attributes);
				model.set(updateObj);
			});

			model.parse = function(response) {
				if (response && response[attributeName]) {
					model[attributeName].reset(response[attributeName]);
				}
				return Backbone.Model.prototype.parse.call(model, response);
			}
			return nestedCollection;
		},

		nonCrudOperation : function(url, requestMethod, options){
			return Backbone.sync.call(this, null, this, _.extend({
				url: url,
				type: requestMethod
			}, options));
		}
	});

	return XABaseModel;
});
