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

 require([
	'backbone',
	'App',
	'RegionManager',
	'routers/Router',
	'controllers/Controller',
	'modules/XAOverrides',
	'hbs!tmpl/common/loading_tmpl'
],
function ( Backbone, App, RegionManager, AppRouter, AppController, XAOverrides,loadingHTML ) {
    'use strict';

	App.appRouter = new AppRouter({
		controller: new AppController()
	});
	App.appRouter.on('beforeroute', function(event) {
		$(App.rContent.$el).html(loadingHTML);
	});
	// Start Marionette Application in desktop mode (default)
	Backbone.fetchCache._cache = {};
	App.start();
});
