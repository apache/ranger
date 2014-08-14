/*
 * Copyright (c) 2014 XASecure
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of
 * Frestone Infotech. ("Confidential Information").  You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Frestone Infotech.
 */

define(function(require) {
    'use strict';

    var Backbone		= require('backbone'); 
    var localization	= require('utils/XALangSupport');
	var SessionMgr		= require('mgrs/SessionMgr');
    /*
     * Localization initialization
     */
    localization.setDefaultCulture(); // will take default that is en
    
	var App = new Backbone.Marionette.Application();
	App.userProfile = SessionMgr.getUserProfile();

	/* Add application regions here */
	App.addRegions({
    	rTopNav			: "#r_topNav",
        rTopProfileBar	: "#r_topProfileBar",
        rBreadcrumbs	: '#r_breadcrumbs',
        rContent		: "#r_content",
        rFooter			: "#r_footer"
	});

	/* Add initializers here */
	App.addInitializer( function () {
	//	Communicator.mediator.trigger("Application:Start");
        Backbone.history.start();
	});

    // Add initialize hooks
    App.on("initialize:before", function() {
    });
	
    App.on("initialize:after", function() {
    });
    
    App.addInitializer(function(options) {
        console.log('Creating new Router instance');
    });
    
    return App;
});

