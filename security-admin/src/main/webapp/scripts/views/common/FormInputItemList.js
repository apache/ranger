/*
 *
 */
define(function(require) {
    'use strict';
    
	var Backbone		= require('backbone');
    var App		        = require('App');
	var XAEnums			= require('utils/XAEnums');
	var XAUtil			= require('utils/XAUtils');
	var localization	= require('utils/XALangSupport');
	var VXGroup			= require('models/VXGroup');
	require('bootstrap-editable');
    	
	var FormInputItem = Backbone.Marionette.ItemView.extend({
		_msvName : 'FormInputItem',
		template : require('hbs!tmpl/common/formInputItem'),
		tagName : 'tr',
		templateHelpers : function(){
			
			return {
				permissions 	: _.flatten(_.pick(XAEnums.XAPermType,  XAUtil.getPerms(this.policyType))),
				policyKnox 		: this.policyType == XAEnums.AssetType.ASSET_KNOX.value ? true :false,
				policyStorm 	: this.policyType == XAEnums.AssetType.ASSET_STORM.value ? true :false,
				isModelNew		: !this.model.has('editMode'),
				stormPerms		: this.stormPermsIds.length == 14 ? _.union(this.stormPermsIds,[-1]) : this.stormPermsIds  
						
			};
		},
		ui : {
			selectGroups	: '[data-js="selectGroups"]',
			inputIPAddress	: '[data-js="ipAddress"]',
			tags			: '[class=tags]'
		},
		events : {
			'click [data-action="delete"]'	: 'evDelete',
			'click td'						: 'evClickTD',
			'change [data-js="selectGroups"]': 'evSelectGroup',
			'change [data-js="ipAddress"]'	: 'evIPAddress'
		},

		initialize : function(options) {
			_.extend(this, _.pick(options, 'groupList','policyType'));
            //this.subjectList = this.mStudent.getSubjectList();
			this.stormPermsIds = [];
			if(this.policyType == XAEnums.AssetType.ASSET_STORM.value){
				if(this.model.has('editMode') && this.model.get('editMode')){
					this.stormPermsIds = _.map(this.model.get('_vPermList'), function(p){return p.permType;});
				}
			}
		},
 
		onRender : function() {
			var that = this;
			if(!_.isUndefined(this.model.get('groupId'))){
				this.ui.selectGroups.val(this.model.get('groupId').split(','));
			}
			if(!_.isUndefined(this.model.get('ipAddress'))){
				this.ui.inputIPAddress.val(this.model.get('ipAddress').toString());
			}
			
			this.createGroupDropDown();
			this.groupDropDownChange();
			if(this.policyType == XAEnums.AssetType.ASSET_STORM.value){
				this.renderStormPerms();
			}
			else{
				// TODO FIXME Remove
				if(this.model.has('editMode') && this.model.get('editMode')){
					_.each(this.model.get('_vPermList'), function(p){
						this.$el.find('input[data-id="' + p.permType + '"]').attr('checked', 'checked');
					},this);
				}
			}
		},
		groupDropDownChange : function(){
			var that = this;
			this.ui.selectGroups.on('change',function(e){
		//		console.log(e.currentTarget.value);
				that.checkDirtyFieldForDropDown(e);
				var duplicateGroupName = false;
				
				that.toggleAddButton(e);
				if(e.removed != undefined){
					var gIdArr = [],gNameArr = [];
					gIdArr = _.without(that.model.get('groupId').split(','), e.removed.id);
					if(that.model.get('groupName') != undefined)
						gNameArr = _.without(that.model.get('groupName').split(','), e.removed.text);
					if(!_.isEmpty(gIdArr)){
						that.model.set('groupId',gIdArr.join(','));
						that.model.set('groupName',gNameArr.join(','));
					}else{
						that.model.unset('groupId');
						that.model.unset('groupName');
					}
					return;
				}
				if(!_.isUndefined(e.added)){
						that.model.set('groupId', e.currentTarget.value);
				}
			});
		},
		createGroupDropDown :function(){
			var that = this;
			if(this.model.has('editMode') && !_.isEmpty(this.ui.selectGroups.val())){
				var temp = this.ui.selectGroups.val().split(",");
				_.each(temp , function(id){
					if(_.isUndefined(that.groupList.get(id))){
						var group = new VXGroup({id : id});
						group.fetch({async:false}).done(function(){
							that.groupList.add(group);
						});
					}
				});
			}
			this.groupArr = this.groupList.map(function(m){
				return { id : m.id+"" , text : m.get('name')};
			});
			this.ui.selectGroups.select2({
				closeOnSelect : true,
				placeholder : 'Select Group',
			//	maximumSelectionSize : 1,
				width :'220px',
				tokenSeparators: [",", " "],
				tags : this.groupArr, 
				initSelection : function (element, callback) {
					var data = [];
					console.log(that.groupList);
					
					$(element.val().split(",")).each(function () {
						var obj = _.findWhere(that.groupArr,{id:this});
					//	if(!_.isUndefined(obj))
							data.push({id: this, text: obj.text});
						/*else{
							var group = new VXGroup({id : this});
							group.fetch().done(function(){
									data.push({id: group.id, text: group.get('name')});
							});
						}*/
					});
					callback(data);
				},
				createSearchChoice: function(term, data) {
				/*	if ($(data).filter(function() {
						return this.text.localeCompare(term) === 0;
					}).length === 0) {
						return {
							id : term,
							text: term
						};
					}*/
				},
				ajax: { 
					url: "service/xusers/groups",
					dataType: 'json',
					data: function (term, page) {
						return {name : term};
					},
					results: function (data, page) { 
						var results = [] , selectedVals = [];
						/*if(!_.isEmpty(that.ui.selectGroups.select2('val')))
							selectedVals = that.ui.selectGroups.select2('val');*/
						selectedVals = that.getGroupSelectdValues();
						if(data.resultSize != "0"){
							//if(data.vXGroups.length > 1){

								results = data.vXGroups.map(function(m, i){	return {id : m.id+"", text: m.name};	});
								if(!_.isEmpty(selectedVals))
									results = XAUtil.filterResultByIds(results, selectedVals);
						//		console.log(results.length);
								return {results : results};
							//}
						//	results = [{id : data.vXGroups.id+"", text: data.vXGroups.name}];
						//	return {results : results};
						}
						return {results : results};
					}
				},	
				formatResult : function(result){
					return result.text;
				},
				formatSelection : function(result){
					return result.text;
				},
				formatNoMatches: function(result){
					return 'No group found.';
				}
			}).on('select2-focus', XAUtil.select2Focus);
		},
		getGroupSelectdValues : function(){
			var vals = [],selectedVals = [];
			this.collection.each(function(m){
				if(!_.isUndefined(m.get('groupId'))){
					vals.push.apply(vals, m.get('groupId').split(','));
				}
			});
			if(!_.isEmpty(this.ui.selectGroups.select2('val')))
				selectedVals = this.ui.selectGroups.select2('val');
			vals.push.apply(vals , selectedVals);
			vals = $.unique(vals);
			return vals;
		},
		evDelete : function(){
			var that = this;
			this.collection.remove(this.model);
			this.toggleAddButton();
		},
		evClickTD : function(e){
			var $el = $(e.currentTarget),permList =[],perms =[];
			if($(e.toElement).is('td')){
				var $checkbox = $el.find('input');
				$checkbox.is(':checked') ? $checkbox.prop('checked',false) : $checkbox.prop('checked',true);
			}
			var curPerm = $el.find('input').data('id');
			var perms = [];
			if(this.model.has('_vPermList')){
				if(_.isArray(this.model.get('_vPermList')))
					perms = this.model.get('_vPermList');
				else
					perms.push(this.model.get('_vPermList'));
			}
			/* permMapList = [ {id: 18, groupId : 1, permType :5}, {id: 18, groupId : 1, permType :4}, {id: 18, groupId : 2, permType :5} ]
			   [1] => [ {id: 18, groupId : 1, permType :5}, {id: 19, groupId : 1, permType :4} ]
			   [2] => [ {id: 20, groupId : 2, permType :5} ]
			{ 	groupId : 1,
				_vPermList : [ { id: 18, permType : 5 }, { id: 19, permType : 4 } ]
			}
			this.model => { 	groupId : 2,
				_vPermList : [ { id: 20, permType : 5 }, { permType : 6 } ]
			}
			
			*/
//			perms = this.model.has('_vPermList') ? this.model.get('_vPermList'): [];
			
			if($el.find('input[type="checkbox"]').is(':checked')){
				perms.push({permType : curPerm});
				if(curPerm == XAEnums.XAPermType.XA_PERM_TYPE_ADMIN.value){
					$el.parent().find('input[type="checkbox"]:not(:checked)[data-id!="'+curPerm+'"]').map(function(){
							perms.push({ permType :$(this).data('id')});
					    //  return { permType :$(this).data('id')};
					});
					$el.parent().find('input[type="checkbox"]').prop('checked',true);
				}
			} else {
				perms = _.reject(perms,function(el) { return el.permType == curPerm; });
			}
			
			this.checkDirtyFieldForCheckBox(perms);
			if(!_.isEmpty(perms))
				this.model.set('_vPermList', perms);
			else 
				this.model.unset('_vPermList');
		},
		checkDirtyFieldForCheckBox : function(perms){
			var permList = [];
			if(!_.isUndefined(this.model.get('_vPermList')))
				permList = _.map(this.model.attributes._vPermList,function(obj){return obj.permType;});
			perms = _.map(perms,function(obj){return obj.permType;});
			XAUtil.checkDirtyField(permList, perms, this.$el);
		},
		toggleAddButton : function(e){
			var temp = [];
			this.collection.each(function(m){
				if(!_.isUndefined(m.get('groupId'))){
					temp.push.apply(temp, m.get('groupId').split(','));
					
				}
			});
			if(!_.isUndefined(e)){
				if( !_.isUndefined(e.added) && ((temp.length + 1) == this.groupList.length)) 
					$('[data-action="addGroup"]').hide();
				if(!_.isUndefined(e.removed))
					$('[data-action="addGroup"]').show();
			}else{
				$('[data-action="addGroup"]').show();
			}
		},
		evIPAddress :function(e){
			if(!_.isEmpty($(e.currentTarget).val()))
				this.model.set('ipAddress',$(e.currentTarget).val().split(','));
			else
				this.model.unset('ipAddress');
		},
		renderStormPerms :function(){
			var that = this;
			var permArr = _.pick(XAEnums.XAPermType,  XAUtil.getPerms(this.policyType));
			this.stormPerms =  _.map(permArr,function(m){return {text:m.label, value:m.value};});
			this.stormPerms.push({'value' : -1, 'text' : 'Select/Deselect All'});
			this.ui.tags.editable({
			    placement: 'right',
//			    emptytext : 'Please select',
			    source: this.stormPerms,
			    display: function(idList,srcData) {
			    	if(_.isEmpty(idList.toString())){
			    		$(this).html('');
			    		return;
			    	}
			    	if(!_.isArray(idList))
			    		idList = [idList];
//			    	that.checkDirtyFieldForGroup(values);
			    	var permTypeArr = [];
		    		var valArr = _.map(idList, function(id){
		    			if(!(parseInt(id) <= 0)){
		    				var obj = _.findWhere(srcData,{'value' : parseInt(id)});
		    				permTypeArr.push({permType : obj.value});
		    				return "<span class='label label-inverse'>" + obj.text + "</span>";
		    			}
		    		});
		    		that.model.set('_vPermList', permTypeArr);
		    		$(this).html(valArr.join(" "));
			    },
			});
			this.$('[id^="tags-edit-"]').click(function(e) {
			    e.stopPropagation();
			    e.preventDefault();
			    that.$('#' + $(this).data('editable') ).editable('toggle');
			    that.$('input[type="checkbox"][value="-1"]').click(function(e){
					var checkboxlist =$(this).closest('.editable-checklist').find('input[type="checkbox"][value!=-1]')
					$(this).is(':checked') ? checkboxlist.prop('checked',true) : checkboxlist.prop('checked',false); 
					
				});
			});
			
		},
		checkDirtyFieldForDropDown : function(e){
			//that.model.has('groupId')
			var groupIdList =[];
			if(!_.isUndefined(this.model.get('groupId')))
				groupIdList = this.model.get('groupId').split(',');
			XAUtil.checkDirtyField(groupIdList, e.val, $(e.currentTarget));
		},

	});



	return Backbone.Marionette.CompositeView.extend({
		_msvName : 'FormInputItemList',
		template : require('hbs!tmpl/common/formInputItemList'),
		//tagName : 'ul', 
		//className : 'timeline-container',
		templateHelpers :function(){
			console.log(XAUtil.getPermHeaders(this.policyType));
			return {
				permHeaders : XAUtil.getPermHeaders(this.policyType)
			};
		},
		getItemView : function(item){
			if(!item){
				return;
			}
			return FormInputItem;
		},
		itemViewContainer : ".js-formInput",
		itemViewOptions : function() {
			return {
				'collection' 	: this.collection,
				'groupList' 	: this.groupList,
				'policyType'	: this.policyType,
			};
		},
		events : {
			'click [data-action="addGroup"]' : 'addNew'
		},
		initialize : function(options) {
			_.extend(this, _.pick(options, 'groupList','policyType'));
			//this.hiveGroupPerm = _.has(options,'hiveGroupPerm') ? true : false;
			this.listenTo(this.groupList, 'sync', this.render, this);
			if(this.collection.length == 0)
				this.collection.add(new Backbone.Model());
		},
		onRender : function(){
			//console.log("onRender of ArtifactFormNoteList called");
			this.toggleAddButton();
		},
		addNew : function(){
			var that =this;
			if(this.groupList.length > this.collection.length){
				this.collection.add(new Backbone.Model());
				this.toggleAddButton();
			}
		},
		toggleAddButton : function(){
			var groupIds=[];
			this.collection.each(function(m){
				if(!_.isUndefined(m.get('groupId'))){
					var temp = m.get('groupId').split(',');
					groupIds.push.apply(groupIds,temp);
				}
			});
			if(groupIds.length == this.groupList.length)
				this.$('button[data-action="addGroup"]').hide();
			else
				this.$('button[data-action="addGroup"]').show();
		},
	});

});
