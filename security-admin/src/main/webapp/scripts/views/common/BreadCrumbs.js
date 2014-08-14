/**
 * 
 * BreadCrumbs Veiw  
 * BreadCrumbs is directly accessible through App object .But can also create a instance of it.
 * @array 
 */

define(function(require) {
	var Marionette	= require('backbone.marionette');
	var tmpl		= require('hbs!tmpl/common/breadcrumbs');
//	var Vent		= new Backbone.Wreqr.EventAggregator();
	var App		= require('App');

	var BreadCrumbs = Marionette.ItemView.extend({
		template : tmpl,
		templateHelpers : function(){
			return {
				breadcrumb : this.setLast(this.breadcrumb)
			};
		},
		initialize : function(options){
			this.breadcrumb = [];
			if(typeof options !== 'undefined'){
				this.breadcrumb = options.breadcrumb;
			}
			//In docs the breadcrubs region stays constant only inner content changes
	/*		this.listenTo(Vent,'draw:docs:breadcrumbs',function(breads){
				this.breadcrumb = breads;
				this.reRenderBookmarks();
			},this);*/
		},
		onRender : function(){

		},
		reRenderBookmarks : function(){
			this.breadcrumb = this.setLast(this.breadcrumb);
			this.render();
		},
		setLast : function(breadcrumb){
			if(breadcrumb.length > 0){
				//breadcrumb[breadcrumb.length -1].isLast = true;
				breadcrumb[breadcrumb.length -1] = _.extend({},breadcrumb[breadcrumb.length -1],{isLast : true});
			}
			return breadcrumb;
		},
		// showBreadCrumbs : function(view,breadcrumb){
			// var brRgn = view.$el.find('#brdCrumbs');
			// if(brRgn){
				// $(brRgn).html(Marionette.Renderer.render(tmpl,{breadcrumb : this.setLast(breadcrumb)}));	
			// }/*else{*/
			   // ////throw new Error('This view does not have a  #brdCrumbs id'); 
			// /*}*/
		// },
		onClose : function(){       
			console.log('OnItemClosed called of BreadCrumbs');
		//	this.stopListening(Vent);
		}
	});

	App.BreadCrumbs = new BreadCrumbs();	

	return BreadCrumbs;
});
