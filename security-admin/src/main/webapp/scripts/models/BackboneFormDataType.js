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

define(function(require) {
	'use strict';

	var Backbone = require('backbone');

	var FormDataType = Backbone.Model.extend({
		type : [ 'string', 'boolean', 'int' ],
		getFormElements : function(configs, enums, attrs, form) {
//			var attrs = [];
			_.each(configs, function(v, k) {
				if (v != null) {
					var formObj = {};
					switch (v.type) {
						case 'string':
							if(!_.isUndefined(v.lookupSupported) && v.lookupSupported ){
								formObj.type = 'Select2Remote';
								formObj.pluginAttr = _.isUndefined(v.url) ? form.getPlugginAttr(false) : form.getPlugginAttr(true, v.url), 
								formObj.editorAttrs = {'data-placeholder': v.label },
								formObj.options = function(callback, editor){
				                    callback();
				                },
				                formObj.onFocusOpen = true
							}else{
								formObj.type = 'Text';
							}
							break;
						case 'bool':
							formObj.type = 'Checkbox';
							formObj.options = {	y : 'Yes',n : 'No'};
							break;
						case 'int':formObj.type = 'Number';break;
						case 'enum':
							var enumObj = _.find(enums, function(e) {return e && e.name == v.subType;});
							formObj.type = 'Select';
							formObj.options = _.pluck(_.compact(enumObj.elements),'label');
							break;
						case 'path' : 
							formObj.type = 'Text';
							form.initilializePathPlugin = true;
							form.pathFieldName = v.name;
							break;
						default:formObj.type = 'Text';break;
					}

					formObj.title = v.label || v.name;
					formObj.validators = [];
					if (_.has(v, 'mandatory') && v.mandatory) {
						formObj.validators.push('required');
						formObj.title = formObj.title + " *"
					}
					formObj['class'] = 'serviceConfig';
					var name = v.name;
					attrs[name] = formObj;
				}
			});
			return attrs;

		}
	});
	return FormDataType;

});
