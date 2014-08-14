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

	var VXAssetBase	= require('model_bases/VXAssetBase');
	var XAUtils		= require('utils/XAUtils');
	var XAEnums		= require('utils/XAEnums');
	var localization= require('utils/XALangSupport');

	var VXAsset = VXAssetBase.extend(
	/** @lends VXAsset.prototype */
	{
		/**
		 * VXAsset initialize method
		 * @augments VXAssetBase
		 * @constructs
		 */
		initialize: function() {
			this.modelName = 'VXAsset';
			this.bindErrorEvents();
		},
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
				name : {
					type		: 'Text',
					title		: 'Repository Name *',
					validators	: ['required'],
					editorAttrs 	:{ maxlength: 255},
				},
				description : {
					type		: 'TextArea',
					title		: 'Description',
					validators	: []
				},
				activeStatus : {
			        type: 'Radio',
					options : function(callback, editor){
						var activeStatus = _.filter(XAEnums.ActiveStatus,function(m){return m.label != 'Deleted'});
						var nvPairs = XAUtils.enumToSelectPairs(activeStatus);
						callback(_.sortBy(nvPairs, function(n){ return !n.val; }));
					}
				},
				assetType : {
					type : 'Select',
					options : function(callback, editor){
						var assetTypes = _.filter(XAEnums.AssetType,function(m){return m.label != 'Unknown'});
						var nvPairs = XAUtils.enumToSelectPairs(assetTypes);
						callback(nvPairs);
					},
					title : localization.tt('lbl.assetType'),
					editorAttrs:{'disabled' : true}
				}

			});
		},

		/** This models toString() */
		toString : function(){
			return this.get('name');
		},
		propertiesNameMap : {
			userName : "username",
			passwordKeytabfile : "password",
			fsDefaultName : "fs.default.name",
			authorization : "hadoop.security.authorization",
			authentication : "hadoop.security.authentication",
			auth_to_local : "hadoop.security.auth_to_local",
			datanode : "dfs.datanode.kerberos.principal",
			namenode : "dfs.namenode.kerberos.principal",
			secNamenode : "dfs.secondary.namenode.kerberos.principal",
			//hive
			driverClassName : "jdbc.driverClassName",
			url	: "jdbc.url",
			
			masterKerberos     		: 'hbase.master.kerberos.principal',
			rpcEngine 				: 'hbase.rpc.engine',
			rpcProtection	 		: 'hbase.rpc.protection',
			securityAuthentication  : 'hbase.security.authentication',
			zookeeperProperty 		: 'hbase.zookeeper.property.clientPort',
			zookeeperQuorum 		: 'hbase.zookeeper.quorum',
			//hbase
			zookeeperZnodeParent	: 'zookeeper.znode.parent',
			//knox
			knoxUrl					:'knox.url',
			
			commonnameforcertificate: 'commonNameForCertificate'
		}

	}, {
		// static class members
	});

    return VXAsset;
	
});


