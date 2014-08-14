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
