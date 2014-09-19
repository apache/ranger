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

	var App 			= require('App');
	var vBreadCrumbs 	= require('views/common/BreadCrumbs');
	var XAEnums			= require('utils/XAEnums');
	
	require('backgrid');
	require('jquery-toggles');
	/**
	HtmlCell renders any html code

	@class Backgrid.HtmlCell
	@extends Backgrid.Cell
	*/
	var HtmlCell = Backgrid.HtmlCell = Backgrid.Cell.extend({
	
		 /** @property */
		 className: "html-cell",
	
		 render: function () {
		     this.$el.empty();
		     var rawValue = this.model.get(this.column.get("name"));
		     var formattedValue = this.formatter.fromRaw(rawValue, this.model);
		     this.$el.append(formattedValue);
		     this.delegateEvents();
		     return this;
		 }
	});
	
	/**
	SwitchCell renders Switch Button

	@class Backgrid.SwitchCell
	@extends Backgrid.Cell
	*/
	
	var SwitchCell = Backgrid.SwitchCell = Backgrid.Cell.extend({
		
		 /** @property */
		 className: "switch-cell",
	
		 initialize: function (options) {
			    UriCell.__super__.initialize.apply(this, arguments);
			    this.switchStatus = options.switchStatus || false;
			    this.click = this.column.get('click') || false;
			    this.drag = this.column.get('drag') || false;
			    this.onText = this.column.get('onText') || 'ON';
			    this.offText = this.column.get('offText') || 'OFF';
			    if (this.column.get("cellClass")) this.$el.addClass(this.column.get("cellClass"));
		  },
			  
		 render: function () {
		     this.$el.empty();
		     if(this.model.get(this.column.get("name")) != undefined){
		    	 rawValue = (this.model.get(this.column.get("name")));
		    	 this.switchStatus = this.formatter.fromRaw(rawValue, this.model);
		     }
		     
		     this.$el.append('<div class="toggle-xa"><div  class="toggle"></div></div>');
		     this.$el.find('.toggle').toggles({on : this.switchStatus,click :this.click,drag:this.drag,
   	    	 								text : {on :this.onText ,off : this.offText}
		     });
		     this.delegateEvents();
		     return this;
		 }
	});

	var UriCell = Backgrid.UriCell = Backgrid.Cell.extend({

		  className: "uri-cell",
		  title: null,
		  target: "_blank",

		  initialize: function (options) {
		    UriCell.__super__.initialize.apply(this, arguments);
		    this.title = options.title || this.title;
		    this.target = options.target || this.target;
		  },

		  render: function () {
		    this.$el.empty();
		    var rawValue = this.model.get(this.column.get("name"));
		    var href = _.isFunction(this.column.get("href")) ? this.column.get('href')(this.model) : this.column.get('href');
		    var klass = this.column.get("klass");
		    var formattedValue = this.formatter.fromRaw(rawValue, this.model);
		    this.$el.append($("<a>", {
		      tabIndex: -1,
		      href: href,
		      title: this.title || formattedValue,
		      'class' : klass 
		    }).text(formattedValue));
		    
		    if(this.column.has("iconKlass")){
		    	var iconKlass = this.column.get("iconKlass");
		    	var iconTitle = this.column.get("iconTitle");
		    	this.$el.find('a').append('<i class="'+iconKlass+'" title="'+iconTitle+'"></i>');
		    }
		    this.delegateEvents();
		    return this;
		  }

	});
	
	 /**
    Renders a checkbox for Provision Table Cell.
    @class Backgrid.SelectCell
    @extends Backgrid.Cell
 */
	 Backgrid.SelectCell = Backgrid.Cell.extend({
	
	   /** @property */
	   className: "select-cell",
	
	   /** @property */
	   tagName: "td",
	
	   /** @property */
	   events: {
	     //"keydown input[type=checkbox]": "onKeydown",
	     "change input[type=checkbox]": "onChange",
	     "click input[type=checkbox]": "enterEditMode"
	   },
	
	   /**
	      Initializer. If the underlying model triggers a `select` event, this cell
	      will change its checked value according to the event's `selected` value.
	
	      @param {Object} options
	      @param {Backgrid.Column} options.column
	      @param {Backbone.Model} options.model
	   */
	   initialize: function (options) {
	
	     this.column = options.column;
	     if (!(this.column instanceof Backgrid.Column)) {
	       this.column = new Backgrid.Column(this.column);
	     }
	
	     if(!this.column.has("enabledVal")){
	     	this.column.set("enabledVal", "true"); // it is not a boolean value for EPM
	     	this.column.set("disabledVal", "false");
	     }
	
	     var column = this.column, model = this.model, $el = this.$el;
	     this.listenTo(column, "change:renderable", function (column, renderable) {
	       $el.toggleClass("renderable", renderable);
	     });
	
	     if (Backgrid.callByNeed(column.renderable(), column, model)) $el.addClass("renderable");
	
	     this.listenTo(model, "change:" + column.get("name"), function () {
	   	if (!$el.hasClass("editor")) this.render();
	     });
	
	     this.listenTo(model, "backgrid:select", function (model, selected) {
	       this.$el.find("input[type=checkbox]").prop("checked", selected).change();
	     });
	
	
	   },
	
	   /**
	      Focuses the checkbox.
	   */
	   enterEditMode: function () {
	     this.$el.find("input[type=checkbox]").focus();
	   },
	
	   /**
	      Unfocuses the checkbox.
	   */
	   exitEditMode: function () {
	     this.$el.find("input[type=checkbox]").blur();
	   },
	
	   /**
	      When the checkbox's value changes, this method will trigger a Backbone
	      `backgrid:selected` event with a reference of the model and the
	      checkbox's `checked` value.
	   */
	   onChange: function () {
	     var checked = this.$el.find("input[type=checkbox]").prop("checked");
	     //this.$el.parent().toggleClass("selected", checked);
	     if(checked)
	    	 this.model.set(this.column.get("name"), XAEnums.ActivationStatus.ACT_STATUS_ACTIVE.value);
	     else
	    	 this.model.set(this.column.get("name"), XAEnums.ActivationStatus.ACT_STATUS_DISABLED.value);
	     this.model.trigger("backgrid:selected", this.model, checked);
	   },
	
	   /**
	      Renders a checkbox in a table cell.
	   */
	   render: function () {
	     var model = this.model, column = this.column;
		  var val = (model.get(column.get("name")) === column.get("enabledVal") || 
				  		model.get(column.get("name")) === XAEnums.ActivationStatus.ACT_STATUS_ACTIVE.value) ? true : false;
	     
	     // this.$el.empty().append('<input tabindex="-1" type="checkbox" />');
	     this.$el.empty();
	
	     // this.$el.find("input[type=checkbox]").prop('checked', val);
	     this.$el.append($("<input>", {
	       tabIndex: -1,
	       type: "checkbox",
	       checked: val
	     }));
	     this.delegateEvents();
	     return this;
	   }
	  });


	
	
	
	 /**
	   * SELECT2
	   *
	   * Renders Select2 - jQuery based replacement for select boxes
	   *
	   * Requires an 'options.values' value on the schema.
	   *  Can be an array of options, a function that calls back with the array of options, a string of HTML
	   *  or a Backbone collection. If a collection, the models must implement a toString() method
	   */
	  var Form =  require('backbone-forms');
	  require('select2');
	  Form.editors.Select2 = Form.editors.Select.extend({		 
	    initialize : function(options){
	      this.pluginAttr = _.extend( {'width' : 'resolve'}, options.schema.pluginAttr || {});
	      Form.editors.Select.prototype.initialize.call(this,options);
	    },
	 
		render: function() {
			var self = this;
			this.setOptions(this.schema.options);
			setTimeout(function () {
			    self.$el.select2(self.pluginAttr);
			},0);			

			return this;
		}
	 
	  });
	  
	  /**
	   * TOGGLE SWITCH
	   * https://github.com/simontabor/jquery-toggles
	   *
	   */
	  Form.editors.Switch = Form.editors.Base.extend({
			ui : {
				
			},
			  events: {
			    'click':  function(event) {
			  this.trigger('change', this);
			},
			'focus':  function(event) {
			  this.trigger('focus', this);
			},
			'blur':   function(event) {
			  this.trigger('blur', this);
			    }
			  },
			
			  initialize: function(options) {
			    Form.editors.Base.prototype.initialize.call(this, options);
				this.template = _.template('<div class="toggle-xa"><div  class="toggle"></div></div>');			
			    //this.$el.attr('type', 'checkbox');
		    	this.switchOn = _.has(this.schema,'switchOn') ?  this.schema.switchOn : false;
		    	this.onText = _.has(this.schema,'onText') ?  this.schema.onText : 'ON';
		    	this.offText = _.has(this.schema,'offText') ?  this.schema.offText : 'OFF';
		    	this.width = _.has(this.schema,'width') ?  this.schema.width : 50;
		    	this.height = _.has(this.schema,'height') ?  this.schema.height : 20;
			  },
			
			  /**
			   * Adds the editor to the DOM
			   */
			  render: function() {
			  	this.$el.html( this.template );
			    this.$el.find('.toggle').toggles({
			    	on:this.switchOn,
			    	text : {on : this.onText, off : this.offText },
			    	width: this.width,
			    	height: this.height
			    });
			
			    return this;
			  },
			
			  getValue: function() {
				  return this.$el.find('.toggle-slide').hasClass('active')? XAEnums.BooleanValue.BOOL_TRUE.value :  XAEnums.BooleanValue.BOOL_FALSE.value ;
				  //return this.$el.find('.active').text() == "ON" ? true : false;
			  },
			
			  setValue: function(switchOn) {
				  this.$el.find('.toggle').toggles({on:switchOn,text : {on : this.onText, off : this.offText }});
				  /*if(switchOn){
					  this.$el.find('.active').removeClass('active');
					  this.$el.find('.toggle-on').addClass('active');
				  }else{
					  this.$el.find('.active').removeClass('active');
					  this.$el.find('.toggle-off').addClass('active');
				  }*/
				  return true;
			  },
			
			  focus: function() {
			    if (this.hasFocus) return;
			
			    this.$el.focus();
			  },
			
			  blur: function() {
			    if (!this.hasFocus) return;
			
			    this.$el.blur();
			  }
			
			});
	  
	  
	  	
	//Scroll to top functionality on all views -- if the scroll height is > 500 px.
	 $(window).scroll(function() {
		if ($(this).scrollTop() > 300) {
				$('#back-top').show();
				$('#back-top').tooltip();
			} else {
				$('#back-top').hide();
			}
		});

		$('#back-top').click(function() {
			$('body,html').animate({
				scrollTop : 0
			}, 800);
			return false;
		});
	  	
	  	
	  
	  
	  /*
		 * Backbone.View override for implementing Breadcrumbs
		 */
		Backbone.View = (function(View) {
		  // Define the new constructor
		  Backbone.View = function(options) {
		    // Call the original constructor
		    View.apply(this, arguments);
		    // Add the render callback
		    if(this.breadCrumbs){
		    	var breadCrumbsArr = [];
		    	if(_.isFunction(this.breadCrumbs))
		    		breadCrumbsArr = this.breadCrumbs();
		    	else
		    		breadCrumbsArr = this.breadCrumbs;
		    		
		    	if(App.rBreadcrumbs.currentView)
		    		App.rBreadcrumbs.close();
			    App.rBreadcrumbs.show(new vBreadCrumbs({breadcrumb:breadCrumbsArr}));
		    }
		  };
		  // 1lone static properties
		  _.extend(Backbone.View, View);
		  // Clone prototype
		  Backbone.View.prototype = (function(Prototype) {
		    Prototype.prototype = View.prototype;
		    return new Prototype;
		  })(function() {});
		  // Update constructor in prototype
		  Backbone.View.prototype.constructor = Backbone.View;
		  return Backbone.View;
		})(Backbone.View);
		
		
		/*
		 * Override val() of jquery to trim values
		 */
		(function ($) {
			var originalVal = $.fn.val;
			 $.fn.val = function(value) { 
			 	if (_.isUndefined(value)) 
					return originalVal.call(this); 
			 	else { 
			 		return originalVal.call(this,(_.isString(value))? $.trim(value):value); 
			 	}
			}; 
		})(jQuery);
		
		/***************   Block UI   ************************/
		/*! Copyright 2011, Ben Lin (http://dreamerslab.com/)
		* Licensed under the MIT License (LICENSE.txt).
		*
		* Version: 1.1.1
		*
		* Requires: jQuery 1.2.6+
		* https://github.com/dreamerslab/jquery.msg/
		*/
		;(function($,window){var get_win_size=function(){if(window.innerWidth!=undefined)return[window.innerWidth,window.innerHeight];else{var B=document.body;var D=document.documentElement;return[Math.max(D.clientWidth,B.clientWidth),Math.max(D.clientHeight,B.clientHeight)]}};$.fn.center=function(opt){var $w=$(window);var scrollTop=$w.scrollTop();return this.each(function(){var $this=$(this);var configs=$.extend({against:"window",top:false,topPercentage:0.5,resize:true},opt);var centerize=function(){var against=configs.against;var against_w_n_h;var $against;if(against==="window")against_w_n_h=get_win_size();else if(against==="parent"){$against=$this.parent();against_w_n_h=[$against.width(),$against.height()];scrollTop=0}else{$against=$this.parents(against);against_w_n_h=[$against.width(),$against.height()];scrollTop=0}var x=(against_w_n_h[0]-$this.outerWidth())*0.5;var y=(against_w_n_h[1]-$this.outerHeight())*configs.topPercentage+scrollTop;if(configs.top)y=configs.top+scrollTop;$this.css({"left":x,"top":y})};centerize();if(configs.resize===true)$w.resize(centerize)})}})(jQuery,window);
		
		/* Copyright 2011, Ben Lin (http://dreamerslab.com/)
		* Licensed under the MIT License (LICENSE.txt).
		*
		* Version: 1.0.7
		*
		* Requires: 
		* jQuery 1.3.0+, 
		* jQuery Center plugin 1.0.0+ https://github.com/dreamerslab/jquery.center
		*/
		;(function(d,e){var a={},c=0,f,b=[function(){}];d.msg=function(){var g,k,j,l,m,i,h;j=[].shift.call(arguments);l={}.toString.call(j);m=d.extend({afterBlock:function(){},autoUnblock:true,center:{topPercentage:0.4},css:{},clickUnblock:true,content:"Please wait...",fadeIn:200,fadeOut:300,bgPath:"",klass:"black-on-white",method:"appendTo",target:"body",timeOut:2400,z:1000},a);l==="[object Object]"&&d.extend(m,j);i={unblock:function(){g=d("#jquery-msg-overlay").fadeOut(m.fadeOut,function(){b[m.msgID](g);g.remove();});clearTimeout(f);}};h={unblock:function(o,n){var p=o===undefined?0:o;m.msgID=n===undefined?c:n;setTimeout(function(){i.unblock();},p);},replace:function(n){if({}.toString.call(n)!=="[object String]"){throw"$.msg('replace'); error: second argument has to be a string";}d("#jquery-msg-content").empty().html(n).center(m.center);},overwriteGlobal:function(o,n){a[o]=n;}};c--;m.msgID=m.msgID===undefined?c:m.msgID;b[m.msgID]=m.beforeUnblock===undefined?function(){}:m.beforeUnblock;if(l==="[object String]"){h[j].apply(h,arguments);}else{g=d('<div id="jquery-msg-overlay" class="'+m.klass+'" style="position:absolute; z-index:'+m.z+"; top:0px; right:0px; left:0px; height:"+d(e).height()+'px;"><img src="'+m.bgPath+'blank.gif" id="jquery-msg-bg" style="width: 100%; height: 100%; top: 0px; left: 0px;"/><div id="jquery-msg-content" class="jquery-msg-content" style="position:absolute;">'+m.content+"</div></div>");g[m.method](m.target);k=d("#jquery-msg-content").center(m.center).css(m.css).hide();g.hide().fadeIn(m.fadeIn,function(){k.fadeIn("fast").children().andSelf().bind("click",function(n){n.stopPropagation();});m.afterBlock.call(h,g);m.clickUnblock&&g.bind("click",function(n){n.stopPropagation();i.unblock();});if(m.autoUnblock){f=setTimeout(i.unblock,m.timeOut);}});}return this;};})(jQuery,document);
		
		/*
		 * BASICS
			**********
			
			dirtyFields is a jQuery plugin that makes a user aware of which form elements have been updated on an HTML form and can reset the form values back to their previous state.
			
			The main website for the plugin (which includes documentation, demos, and a download file) is currently at:
	
			http://www.thoughtdelimited.org/dirtyFields/index.cfm
		 * 
		 */
		
		(function(e){function t(t,n,r){var i=n.data("dF").dirtyFieldsDataProperty;var s=e.inArray(t,i);if(r=="dirty"&&s==-1){i.push(t);n.data("dF").dirtyFieldsDataProperty=i}else if(r=="clean"&&s>-1){i.splice(s,1);n.data("dF").dirtyFieldsDataProperty=i}}function n(t){if(t.data("dF").dirtyFieldsDataProperty.length>0){t.addClass(t.data("dF").dirtyFormClass);if(e.isFunction(t.data("dF").formChangeCallback)){t.data("dF").formChangeCallback.call(t,true,t.data("dF").dirtyFieldsDataProperty)}}else{t.removeClass(t.data("dF").dirtyFormClass);if(e.isFunction(t.data("dF").formChangeCallback)){t.data("dF").formChangeCallback.call(t,false,t.data("dF").dirtyFieldsDataProperty)}}}function r(t,n,r,i){if(i.data("dF").denoteDirtyFields){var s=i.data("dF").fieldOverrides;var o=n.attr("id");var u=false;for(var a in s){if(o==a){if(r=="changed"){e("#"+s[a]).addClass(i.data("dF").dirtyFieldClass)}else{e("#"+s[a]).removeClass(i.data("dF").dirtyFieldClass)}u=true}}if(u==false){var f=i.data("dF")[t];var l=f.split("-");switch(l[0]){case"next":if(r=="changed"){n.next(l[1]).addClass(i.data("dF").dirtyFieldClass)}else{n.next(l[1]).removeClass(i.data("dF").dirtyFieldClass)}break;case"previous":if(r=="changed"){n.prev(l[1]).addClass(i.data("dF").dirtyFieldClass)}else{n.prev(l[1]).removeClass(i.data("dF").dirtyFieldClass)}break;case"closest":if(r=="changed"){n.closest(l[1]).addClass(i.data("dF").dirtyFieldClass)}else{n.closest(l[1]).removeClass(i.data("dF").dirtyFieldClass)}break;case"self":if(r=="changed"){n.addClass(i.data("dF").dirtyFieldClass)}else{n.removeClass(i.data("dF").dirtyFieldClass)}break;default:if(l[0]=="id"||l[0]=="name"){switch(l[1]){case"class":if(r=="changed"){e("."+n.attr(l[0]),i).addClass(i.data("dF").dirtyFieldClass)}else{e("."+n.attr(l[0]),i).removeClass(i.data("dF").dirtyFieldClass)}break;case"title":if(r=="changed"){e("*[title='"+n.attr(l[0])+"']",i).addClass(i.data("dF").dirtyFieldClass)}else{e("*[title='"+n.attr(l[0])+"']",i).removeClass(i.data("dF").dirtyFieldClass)}break;case"for":if(r=="changed"){e("label[for='"+n.attr(l[0])+"']",i).addClass(i.data("dF").dirtyFieldClass)}else{e("label[for='"+n.attr(l[0])+"']",i).removeClass(i.data("dF").dirtyFieldClass)}break}}break}}}}function i(i,s){var o=i.attr("name");var u=false;if(s.data("dF").trimText){var a=jQuery.trim(i.val())}else{var a=i.val()}if(i.hasClass(s.data("dF").ignoreCaseClass)){var a=a.toUpperCase();var f=i.data(s.data("dF").startingValueDataProperty).toUpperCase()}else{var f=i.data(s.data("dF").startingValueDataProperty)}if(a!=f){r("textboxContext",i,"changed",s);t(o,s,"dirty");u=true}else{r("textboxContext",i,"unchanged",s);t(o,s,"clean")}if(e.isFunction(s.data("dF").fieldChangeCallback)){s.data("dF").fieldChangeCallback.call(i,i.data(s.data("dF").startingValueDataProperty),u)}if(s.data("dF").denoteDirtyForm){n(s)}}function s(i,s){var o=i.attr("name");var u=false;if(s.data("dF").denoteDirtyOptions==false&&i.attr("multiple")!=true){if(i.hasClass(s.data("dF").ignoreCaseClass)){var a=i.val().toUpperCase();var f=i.data(s.data("dF").startingValueDataProperty).toUpperCase()}else{var a=i.val();var f=i.data(s.data("dF").startingValueDataProperty)}if(a!=f){r("selectContext",i,"changed",s);t(o,s,"dirty");u=true}else{r("selectContext",i,"unchanged",s);t(o,s,"clean")}}else{var l=false;i.children("option").each(function(t){var n=e(this);var r=n.is(":selected");if(r!=n.data(s.data("dF").startingValueDataProperty)){if(s.data("dF").denoteDirtyOptions){n.addClass(s.data("dF").dirtyOptionClass)}l=true}else{if(s.data("dF").denoteDirtyOptions){n.removeClass(s.data("dF").dirtyOptionClass)}}});if(l){r("selectContext",i,"changed",s);t(o,s,"dirty");u=true}else{r("selectContext",i,"unchanged",s);t(o,s,"clean")}}if(e.isFunction(s.data("dF").fieldChangeCallback)){s.data("dF").fieldChangeCallback.call(i,i.data(s.data("dF").startingValueDataProperty),u)}if(s.data("dF").denoteDirtyForm){n(s)}}function o(i,s){var o=i.attr("name");var u=false;var a=i.attr("type");e(":"+a+"[name='"+o+"']",s).each(function(t){var n=e(this);var i=n.is(":checked");if(i!=n.data(s.data("dF").startingValueDataProperty)){r("checkboxRadioContext",n,"changed",s);u=true}else{r("checkboxRadioContext",n,"unchanged",s)}});if(u){t(o,s,"dirty")}else{t(o,s,"clean")}if(e.isFunction(s.data("dF").fieldChangeCallback)){s.data("dF").fieldChangeCallback.call(i,i.data(s.data("dF").startingValueDataProperty),u)}if(s.data("dF").denoteDirtyForm){n(s)}}e.fn.dirtyFields=function(t){var n=e.extend({},e.fn.dirtyFields.defaults,t);return this.each(function(){var t=e(this);t.data("dF",n);t.data("dF").dirtyFieldsDataProperty=new Array;e("input[type='text'],input[type='file'],input[type='password'],textarea",t).not("."+t.data("dF").exclusionClass).each(function(n){e.fn.dirtyFields.configureField(e(this),t,"text")});e("select",t).not("."+t.data("dF").exclusionClass).each(function(n){e.fn.dirtyFields.configureField(e(this),t,"select")});e(":checkbox,:radio",t).not("."+t.data("dF").exclusionClass).each(function(n){e.fn.dirtyFields.configureField(e(this),t,"checkRadio")});e.fn.dirtyFields.setStartingValues(t)})};e.fn.dirtyFields.defaults={checkboxRadioContext:"next-span",denoteDirtyOptions:false,denoteDirtyFields:true,denoteDirtyForm:false,dirtyFieldClass:"dirtyField",dirtyFieldsDataProperty:"dirtyFields",dirtyFormClass:"dirtyForm",dirtyOptionClass:"dirtyOption",exclusionClass:"dirtyExclude",fieldChangeCallback:"",fieldOverrides:{none:"none"},formChangeCallback:"",ignoreCaseClass:"dirtyIgnoreCase",preFieldChangeCallback:"",selectContext:"id-for",startingValueDataProperty:"startingValue",textboxContext:"id-for",trimText:false};e.fn.dirtyFields.configureField=function(t,n,r,u){if(!t.hasClass(n.data("dF").exclusionClass)){if(typeof u!="undefined"){n.data("dF").fieldOverrides[t.attr("id")]=u}switch(r){case"text":t.change(function(){if(e.isFunction(n.data("dF").preFieldChangeCallback)){if(n.data("dF").preFieldChangeCallback.call(t,t.data(n.data("dF").startingValueDataProperty))==false){return false}}i(t,n)});break;case"select":t.change(function(){if(e.isFunction(n.data("dF").preFieldChangeCallback)){if(n.data("dF").preFieldChangeCallback.call(t,t.data(n.data("dF").startingValueDataProperty))==false){return false}}s(t,n)});break;case"checkRadio":t.change(function(){if(e.isFunction(n.data("dF").preFieldChangeCallback)){if(n.data("dF").preFieldChangeCallback.call(t,t.data(n.data("dF").startingValueDataProperty))==false){return false}}o(t,n)});break}}};e.fn.dirtyFields.formSaved=function(t){e.fn.dirtyFields.setStartingValues(t);e.fn.dirtyFields.markContainerFieldsClean(t)};e.fn.dirtyFields.markContainerFieldsClean=function(t){var n=new Array;t.data("dF").dirtyFieldsDataProperty=n;e("."+t.data("dF").dirtyFieldClass,t).removeClass(t.data("dF").dirtyFieldClass);if(t.data("dF").denoteDirtyOptions){e("."+t.data("dF").dirtyOptionClass,t).removeClass(t.data("dF").dirtyOptionClass)}if(t.data("dF").denoteDirtyForm){t.removeClass(t.data("dF").dirtyFormClass)}};e.fn.dirtyFields.setStartingValues=function(t,n){e("input[type='text'],input[type='file'],input[type='password'],:checkbox,:radio,textarea",t).not("."+t.data("dF").exclusionClass).each(function(n){var r=e(this);if(r.attr("type")=="radio"||r.attr("type")=="checkbox"){e.fn.dirtyFields.setStartingCheckboxRadioValue(r,t)}else{e.fn.dirtyFields.setStartingTextValue(r,t)}});e("select",t).not("."+t.data("dF").exclusionClass).each(function(n){e.fn.dirtyFields.setStartingSelectValue(e(this),t)})};e.fn.dirtyFields.setStartingTextValue=function(t,n){return t.not("."+n.data("dF").exclusionClass).each(function(){var t=e(this);t.data(n.data("dF").startingValueDataProperty,t.val())})};e.fn.dirtyFields.setStartingCheckboxRadioValue=function(t,n){return t.not("."+n.data("dF").exclusionClass).each(function(){var t=e(this);var r;if(t.is(":checked")){t.data(n.data("dF").startingValueDataProperty,true)}else{t.data(n.data("dF").startingValueDataProperty,false)}})};e.fn.dirtyFields.setStartingSelectValue=function(t,n){return t.not("."+n.data("dF").exclusionClass).each(function(){var t=e(this);if(n.data("dF").denoteDirtyOptions==false&&t.attr("multiple")!=true){t.data(n.data("dF").startingValueDataProperty,t.val())}else{var r=new Array;t.children("option").each(function(t){var i=e(this);if(i.is(":selected")){i.data(n.data("dF").startingValueDataProperty,true);r.push(i.val())}else{i.data(n.data("dF").startingValueDataProperty,false)}});t.data(n.data("dF").startingValueDataProperty,r)}})};e.fn.dirtyFields.rollbackTextValue=function(t,n,r){if(typeof r=="undefined"){r=true}return t.not("."+n.data("dF").exclusionClass).each(function(){var t=e(this);t.val(t.data(n.data("dF").startingValueDataProperty));if(r){i(t,n)}})};e.fn.dirtyFields.updateTextState=function(t,n){return t.not("."+n.data("dF").exclusionClass).each(function(){i(e(this),n)})};e.fn.dirtyFields.rollbackCheckboxRadioState=function(t,n,r){if(typeof r=="undefined"){r=true}return t.not("."+n.data("dF").exclusionClass).each(function(){var t=e(this);if(t.data(n.data("dF").startingValueDataProperty)){t.attr("checked",true)}else{t.attr("checked",false)}if(r){o(t,n)}})};e.fn.dirtyFields.updateCheckboxRadioState=function(t,n){return t.not("."+n.data("dF").exclusionClass).each(function(){o(e(this),n)})};e.fn.dirtyFields.rollbackSelectState=function(t,n,r){if(typeof r=="undefined"){r=true}return t.not("."+n.data("dF").exclusionClass).each(function(){var t=e(this);if(n.data("dF").denoteDirtyOptions==false&&t.attr("multiple")!=true){t.val(t.data(n.data("dF").startingValueDataProperty))}else{t.children("option").each(function(t){var r=e(this);if(r.data(n.data("dF").startingValueDataProperty)){r.attr("selected",true)}else{r.attr("selected",false)}})}if(r){s(t,n)}})};e.fn.dirtyFields.updateSelectState=function(t,n){return t.not("."+n.data("dF").exclusionClass).each(function(){s(e(this),n)})};e.fn.dirtyFields.rollbackForm=function(t){e("input[type='text'],input[type='file'],input[type='password'],:checkbox,:radio,textarea",t).not("."+t.data("dF").exclusionClass).each(function(n){$object=e(this);if($object.attr("type")=="radio"||$object.attr("type")=="checkbox"){e.fn.dirtyFields.rollbackCheckboxRadioState($object,t,false)}else{e.fn.dirtyFields.rollbackTextValue($object,t,false)}});e("select",t).not("."+t.data("dF").exclusionClass).each(function(n){e.fn.dirtyFields.rollbackSelectState(e(this),t,false)});e.fn.dirtyFields.markContainerFieldsClean(t)};e.fn.dirtyFields.updateFormState=function(t){e("input[type='text'],input[type='file'],input[type='password'],:checkbox,:radio,textarea",t).not("."+t.data("dF").exclusionClass).each(function(n){$object=e(this);if($object.attr("type")=="radio"||$object.attr("type")=="checkbox"){e.fn.dirtyFields.updateCheckboxRadioState($object,t)}else{e.fn.dirtyFields.updateTextState($object,t)}});e("select",t).not("."+t.data("dF").exclusionClass).each(function(n){$object=e(this);e.fn.dirtyFields.updateSelectState($object,t)})};e.fn.dirtyFields.getDirtyFieldNames=function(e){return e.data("dF").dirtyFieldsDataProperty};})(jQuery)
});
