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
	var localization	= require('utils/XALangSupport');

	require('backbone-forms');
	require('backbone-forms.list');
	require('backbone-forms.templates');
	require('backbone-forms.XAOverrides');

	var AssetForm = Backbone.Form.extend(
	/** @lends AssetForm */
	{
		_viewName : 'AssetForm',

    	/**
		* intialize a new AssetForm Form View 
		* @constructs
		*/
		initialize: function(options) {
			console.log("initialized a AssetForm Form View");
    		Backbone.Form.prototype.initialize.call(this, options);

			_.extend(this, _.pick(options, ''));

			this.bindEvents();
		},

		/** all events binding here */
		bindEvents : function(){
			this.on('assetType:change', function(form, fieldEditor){
    			this.evFieldChange(form, fieldEditor);
    		});
		},

		/** schema for the form
		* If you see that a field should behave similarly in all the forms, its 
		* better to make the change in this.model.schema itself
		*
		* Override here ONLY if special case!!
		*/

		fields: ['name', 'description', 'activeStatus', 'assetType','config', '_vPassword'],

		schema : function(){

			var attrs = _.pick(_.result(this.model,'schemaBase'), 'name', 'description', 'activeStatus', 'assetType', 'config');

			attrs._vPassword = {
				type		: 'Password',
				title		: localization.tt("lbl.assetConfigPass")
			};
			$.extend(attrs,{
				userName : {
					type : 'Text',
//					fieldClass : "hdfs hive knox",
					title : this.model.propertiesNameMap.userName+" *",//'xalogin.username'
					validators  : ['required'],//{type:'regexp',regexp:/^[a-z][a-z0-9,._'-]+$/i,message :'Please enter valid username'}],
					editorAttrs :{'class':'stretchTextInput'}//,'maxlength': 48}
				},
				passwordKeytabfile : {
					type : 'Password',
//					fieldClass : "hdfs hive knox",
					title : this.model.propertiesNameMap.passwordKeytabfile+" *",//'xalogin.password'
					validators:['required'],
					editorAttrs :{'class':'stretchTextInput'}
				},
				fsDefaultName : {
					fieldClass : "hdfs",
					title : this.model.propertiesNameMap.fsDefaultName +" *",//'core-site.fs.default.name',
					validators:['required'],
					            /*{type:'regexp',regexp:new RegExp('(hdfs://)\\s*(.*?):[0-9]{1,5}'),
								message :localization.tt('serverMsg.fsDefaultNameValidationError')}],*/
					editorAttrs :{'class':'stretchTextInput'}								
							
					
				},
				authorization : {
					fieldClass : "hdfs",
					title : this.model.propertiesNameMap.authorization,//'core-site.hadoop.security.authorization',
					editorAttrs :{'class':'stretchTextInput'}
				},
				authentication : {
					fieldClass : "hdfs",
					title : this.model.propertiesNameMap.authentication,//'core-site.hadoop.security.authentication'
					editorAttrs :{'class':'stretchTextInput'}
				},
				auth_to_local : {
					fieldClass : "hdfs",
					title : this.model.propertiesNameMap.auth_to_local,//'core-site.hadoop.security.auth_to_local'
					editorAttrs :{'class':'stretchTextInput'}
				},
				datanode : {
					title		: this.model.propertiesNameMap.datanode,//'hdfs-site.dfs.datanode.kerberos.principal',
					fieldClass : "hdfs",
					editorAttrs :{'class':'stretchTextInput'}
				},
				namenode : {
					title		: this.model.propertiesNameMap.namenode,//'hdfs-site.dfs.namenode.kerberos.principal',
					fieldClass : "hdfs",
					editorAttrs :{'class':'stretchTextInput'}
				},
				secNamenode : {
					title		: this.model.propertiesNameMap.secNamenode,//'hdfs-site.dfs.secondary.namenode.kerberos.principal',
					fieldClass : "hdfs",
					editorAttrs :{'class':'stretchTextInput'}
				},
				driverClassName : {
					fieldClass : "hive",
					title : this.model.propertiesNameMap.driverClassName,//'xalogin.jdbc.driverClassName'
					editorAttrs :{'class':'stretchTextInput'}
				},
				url : {
					fieldClass : "hive",
					title : this.model.propertiesNameMap.url,//'xalogin.jdbc.url'
					editorAttrs :{'class':'stretchTextInput'}
				},
				masterKerberos : {
					fieldClass : "hbase",
					title : this.model.propertiesNameMap.masterKerberos,
					editorAttrs :{'class':'stretchTextInput'}
				},
				rpcEngine : {
					fieldClass : "hbase",
					title : this.model.propertiesNameMap.rpcEngine,
					editorAttrs :{'class':'stretchTextInput'}
				},
				rpcProtection : {
					fieldClass : "hbase",
					title : this.model.propertiesNameMap.rpcProtection,
					editorAttrs :{'class':'stretchTextInput'}
				},
				securityAuthentication : {
					fieldClass : "hbase",
					title : this.model.propertiesNameMap.securityAuthentication,
					editorAttrs :{'class':'stretchTextInput'}
				},
				zookeeperProperty : {
					fieldClass : "hbase",
					title : this.model.propertiesNameMap.zookeeperProperty,
					editorAttrs :{'class':'stretchTextInput'}
				},
				zookeeperQuorum : {
					fieldClass : "hbase",
					title : this.model.propertiesNameMap.zookeeperQuorum,
					editorAttrs :{'class':'stretchTextInput'}
				},
				zookeeperZnodeParent : {
					fieldClass : "hbase",
					title : this.model.propertiesNameMap.zookeeperZnodeParent,
					editorAttrs :{'class':'stretchTextInput'}
				},
				knoxUrl : {
					fieldClass : "knox",
					title : this.model.propertiesNameMap.knoxUrl,
					editorAttrs :{'class':'stretchTextInput'}
				},
				nimbusUrl : {
					fieldClass : "storm",
					title : this.model.propertiesNameMap.nimbusUrl,
					editorAttrs :{'class':'stretchTextInput'}
				},
				commonnameforcertificate : {
					title : localization.tt('lbl.commonNameForCertificate'),
					editorAttrs :{'class':'stretchTextInput'}
				}
			});
			return attrs;
		},

		/** on render callback */
		render: function(options) {
			Backbone.Form.prototype.render.call(this, options);

			this.initializePlugins();
			this.renderCustomFields();
			if(!this.model.isNew())
				this.fields.assetType.editor.$el.prop('disabled',true);
			else
				this.fields.activeStatus.editor.setValue(XAEnums.ActiveStatus.STATUS_ENABLED.value);
		},

		/** all custom field rendering */
		renderCustomFields: function(){
			/*this.$('[data-customfields="field1"]').append(new fieldView({
			}).render().el);*/
			//TODO FIXME 
			
			if(this.model)
				this.assetTypeChanged(this.model.get('assetType'));
		},

		/** all post render plugin initialization */
		initializePlugins: function(){
		},

		formValidation : function(){
			//return false;
			return true;
		},

		beforeSave : function(){
			var that = this;
			//this.model.set('assetId', XAGlobals.hardcoded.HDFSAssetId);
//			var attrs = ['userName','passwordKeytabfile','fsDefaultName' ,'authorization', 'authentication', 'auth_to_local', 'datanode', 'namenode', 'secNamenode',
//							'driverClassName', 'url'];
			var attrs;
			switch(parseInt(this.model.get('assetType'))){
				case XAEnums.AssetType.ASSET_HDFS.value :
					attrs = ['userName','passwordKeytabfile','fsDefaultName' ,'authorization', 'authentication', 'auth_to_local', 'datanode', 'namenode', 'secNamenode',
								'commonnameforcertificate'];
					break;
				case XAEnums.AssetType.ASSET_HIVE.value :
					attrs = ['userName','passwordKeytabfile','driverClassName', 'url','commonnameforcertificate'];
					break;
				case XAEnums.AssetType.ASSET_HBASE.value :
					attrs = ['userName','passwordKeytabfile','fsDefaultName' ,'authorization', 'authentication', 'auth_to_local', 'datanode', 'namenode', 'secNamenode',
								'masterKerberos','rpcEngine','rpcProtection','securityAuthentication','zookeeperProperty','zookeeperQuorum','zookeeperZnodeParent','commonnameforcertificate'];
					break;
				case XAEnums.AssetType.ASSET_KNOX.value :
					attrs = ['userName','passwordKeytabfile','knoxUrl','commonnameforcertificate'];
					break;
				case XAEnums.AssetType.ASSET_STORM.value :
					attrs = ['userName','passwordKeytabfile','nimbusUrl','commonnameforcertificate'];
			}
			var obj = _.pick(this.model.attributes,attrs);
			_.each(obj,function(val,prop){
				obj[that.model.propertiesNameMap[prop]] = obj[prop];
				delete obj[prop];
				this.model.unset(prop);
			},this);
			this.model.set('config',JSON.stringify(obj));
		},
		evFieldChange : function(form, fieldEditor){
			this.assetTypeChanged(fieldEditor.getValue());
		},
		assetTypeChanged : function(val){
			this.$('.hive').parents('fieldset').show();
			this.$('.hdfs,.hive,.hbase,.knox,.storm').hide();
			switch(parseInt(val)){
				case XAEnums.AssetType.ASSET_HDFS.value :
					this.fields.fsDefaultName.$el.find('.control-label').html(this.model.propertiesNameMap.fsDefaultName+'*');
//					this.fields.fsDefaultName.editor.validators.push('required');
					this.$('.hdfs').show();
					break;
				case XAEnums.AssetType.ASSET_HIVE.value :
					this.fields.fsDefaultName.editor.validators = this.removeElementFromArr(this.fields.fsDefaultName.editor.validators , 'required');
					this.$('.hive').show();
					break;
				case XAEnums.AssetType.ASSET_HBASE.value :
					//	this.$('.hive').parents('fieldset').hide();
					this.fields.fsDefaultName.$el.find('.control-label').html(this.model.propertiesNameMap.fsDefaultName);
					this.fields.fsDefaultName.$el.removeClass('error');
					this.fields.fsDefaultName.$el.find('.help-inline').html('');
//					this.fields.fsDefaultName.editor.$el.val('');
					this.fields.fsDefaultName.editor.validators = this.removeElementFromArr(this.fields.fsDefaultName.editor.validators , 'required');
					//Set default value to zookeeperZnodeParent
					if(this.model.isNew())
						this.fields.zookeeperZnodeParent.editor.$el.val('/hbase');
					this.$('.hdfs').show();
					this.$('.hive').show();//parents('fieldset').show();
					this.fields.driverClassName.$el.hide();
					this.fields.url.$el.hide();
					this.$('.hbase').show();
					break;
				case XAEnums.AssetType.ASSET_KNOX.value :
					this.fields.fsDefaultName.editor.validators = this.removeElementFromArr(this.fields.fsDefaultName.editor.validators , 'required');
					this.$('.knox').show();
					break;
				case XAEnums.AssetType.ASSET_STORM.value :
					this.fields.fsDefaultName.editor.validators = this.removeElementFromArr(this.fields.fsDefaultName.editor.validators , 'required');
					this.$('.storm').show();
					break;	
				
			}
	/*		if(val == XAEnums.AssetType.ASSET_HDFS.value){
				this.fields.fsDefaultName.$el.find('.control-label').html(this.model.propertiesNameMap.fsDefaultName+'*');
				this.fields.fsDefaultName.editor.validators.push('required');
				this.$('.hdfs').show();
			}else if(val == XAEnums.AssetType.ASSET_HIVE.value){
				this.fields.fsDefaultName.editor.validators = this.removeElementFromArr(this.fields.fsDefaultName.editor.validators , 'required');
				this.$('.hive').show();
			}else{
			//	this.$('.hive').parents('fieldset').hide();
				this.fields.fsDefaultName.$el.find('.control-label').html(this.model.propertiesNameMap.fsDefaultName);
				this.fields.fsDefaultName.$el.removeClass('error');
				this.fields.fsDefaultName.$el.find('.help-inline').html('');
//				this.fields.fsDefaultName.editor.$el.val('');
				this.fields.fsDefaultName.editor.validators = this.removeElementFromArr(this.fields.fsDefaultName.editor.validators , 'required');
				//Set default value to zookeeperZnodeParent
				if(this.model.isNew())
					this.fields.zookeeperZnodeParent.editor.$el.val('/hbase');
				this.$('.hdfs').show();
				this.$('.hive').show();//parents('fieldset').show();
				this.fields.driverClassName.$el.hide();
				this.fields.url.$el.hide();
				this.$('.hbase').show();
			}*/
			this.fields.userName.setValue('');
			this.fields.passwordKeytabfile.setValue('');
			if(! this.model.isNew()){
				if(val == this.model.get('assetType') && this.model.get('config')){
					var configObj=$.parseJSON(this.model.get('config'));
					this.fields.userName.setValue(configObj[this.model.propertiesNameMap.userName]);
					this.fields.passwordKeytabfile.setValue(configObj[this.model.propertiesNameMap.passwordKeytabfile]);
				}
			}
			
		},
		removeElementFromArr : function(arr ,elem){
			var index = $.inArray(elem,arr);
			if(index >= 0) arr.splice(index,1);
			return arr;
		}
	});

	return AssetForm;
});
