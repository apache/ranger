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

 /*
 *
 */
define(function(require) {
    'use strict';
    
    var Backbone        = require('backbone');
    var App             = require('App');
    var XAEnums         = require('utils/XAEnums');
    var XAUtil          = require('utils/XAUtils');
    var localization    = require('utils/XALangSupport');
    var ServiceAuditFilterResourcesForm = require('views/service/ServiceAuditFilterResources');
    var RangerPolicyResource = require('models/RangerPolicyResource');
    require('bootstrap-editable');
            
    var AuditConfigItem = Backbone.Marionette.ItemView.extend({
        _msvName : 'FormInputItem',

        template : require('hbs!tmpl/service/AuditFilterItem_tmpl'),

        tagName : 'tr',

        templateHelpers : function(){},

        ui : {
            selectUsers     : '[data-js="selectUsers"]',
            selectGroups    : '[data-js="selectGroups"]',
            selectRoles    : '[data-js="selectRoles"]',
            addPerms        : 'a[data-js="permissions"]',
            addPermissionsSpan : '.add-permissions',
            addResources : '[data-js="serviceResource"]',
            removeResources : '[data-action="deleteResources"]',
            oparations       : '[data-js="oparations"]',
            accessResult : '[data-js="accessResult"]',
        },
        events : {
            'click [data-action="delete"]'  : 'evDelete',
            'change [data-js="isAudited"]': 'evIsAudited',
            'change [data-js="accessResult"]': 'evAccessResult',
            'click [data-js="serviceResource"]'  : 'eGetResources',
            'click [data-action="deleteResources"]' : 'eRemoveResources',
            'change [data-js="selectUsers"]': 'evSelectUsers',
            'change [data-js="selectGroups"]': 'evSelectGroup',
            'change [data-js="selectRoles"]': 'evSelectRoles',
            'change [data-js="oparations"]': 'evOparations',
        },

        initialize : function(options) {
            _.extend(this, _.pick(options,'rangerServiceDefModel', 'accessTypes', 'serviceName'));
        },
 
        onRender : function() {
            var accessResultVal = _.map(XAEnums.AccessResult, function(m){
                return {'id' : m.label ,'text': m.label}
            })
            if (this.model && !_.isUndefined(this.model.get('accessResult'))) {
                this.ui.accessResult.val(this.model.get('accessResult'));
            }
            this.ui.accessResult.select2({
                placeholder: "Select Value",
                allowClear: true,
                maximumSelectionSize : 1,
                data: accessResultVal,
                theme: 'bootstrap4',
            });
            this.setupFormForEditMode();
            this.evIsAudited();
            this.renderPermissions();
            this.ui.selectUsers.select2(XAUtil.getUsersGroupsList("users", this, '250px', "auditFilter"));
            this.ui.selectGroups.select2(XAUtil.getUsersGroupsList("groups", this, '250px', "auditFilter"));
            this.ui.selectRoles.select2(XAUtil.getUsersGroupsList("roles", this, '250px', "auditFilter"));
            this.selectOparations();
        },

        setupFormForEditMode : function () {
            var that = this;
            if(!this.model.isEmpty()) {
                if (this.model.get('isAudited')) {
                    this.$el.find('[data-js="isAudited"]').val(this.model.get('isAudited'));
                }
                if (this.model.get('users')) {
                    this.ui.selectUsers.val(_.map(this.model.get('users'), function(name){ return _.escape(name)}));
                }
                if (this.model.get('groups')) {
                    this.ui.selectGroups.val(_.map(this.model.get('groups'), function(name){ return _.escape(name); }));
                }
                if (this.model.get('roles')) {
                    this.ui.selectRoles.val(_.map(this.model.get('roles'), function(name){ return _.escape(name); }));
                }
                if (this.model.get('accessTypes')) {
                    _.each(this.model.get('accessTypes'), function(val) {
                        that.$el.find('input[data-name="' + val + '"]').prop('checked', true);
                    })
                }
                if (this.model.get('resources')) {
                    this.displayResources(this.model.get('resources'));
                }
            }
        },

        evIsAudited : function (e) {
            if(_.isUndefined(e)) {
                this.model.set('isAudited',this.$el.find('[data-js="isAudited"]').val());
            }else {
                this.model.set('isAudited', e.currentTarget.value);
            }
        },

        evAccessResult : function (e){
            if($(e.currentTarget).val() != "") {
                this.model.set('accessResult', e.currentTarget.value);
            } else {
                this.model.unset('accessResult');
            }
        },

        evSelectUsers : function (e) {
            this.model.set('users', e.val)
        },

        evSelectGroup : function (e) {
            this.model.set('groups', e.val)
        },

        evSelectRoles : function (e) {
            this.model.set('roles', e.val)
        },

        evOparations : function (e) {
            this.model.set('actions', e.val)
        },

        renderPermissions : function() {
            this.permsIds = [];
            var that = this;
            this.perms =  _.map(this.accessTypes , function(m){return {text : m.label, value : m.name};});
            if(this.perms.length > 1){
                this.perms.push({'value' : -1, 'text' : 'Select/Deselect All'});
            }
            if (this.model && this.model.get('accessTypes')) {
                this.permsIds = this.model.get('accessTypes');
            }
            this.ui.addPerms.editable({
                emptytext : 'Add Permissions',
                source: this.perms,
                value : this.permsIds,
                display: function(values,srcData) {
                    if(_.isNull(values) || _.isEmpty(values) || (_.contains(values,"-1")  &&  values.length == 1)){
                        $(this).empty();
                        that.model.unset('accessTypes');
                        that.ui.addPermissionsSpan.find('i').attr('class', 'fa-fw fa fa-plus');
                        that.ui.addPermissionsSpan.attr('title','add');
                        return;
                    }
                    if(_.contains(values,"-1")){
                        values = _.without(values,"-1")
                    }
                    //that.checkDirtyFieldForGroup(values);
                    var permTypeArr = [];
                    var valArr = _.map(values, function(id){
                        if(!_.isUndefined(id)){
                            var obj = _.findWhere(that.rangerServiceDefModel.attributes.accessTypes,{'name' : id});
                            permTypeArr.push({permType : obj.value});
                            return "<span class='badge badge-info'>" + obj.label + "</span>";
                        }
                    });
                    that.model.set('accessTypes', values);
                    $(this).html(valArr.join(" "));
                    that.ui.addPermissionsSpan.find('i').attr('class', 'fa-fw fa fa-pencil');
                    that.ui.addPermissionsSpan.attr('title','edit');
                }}).on('shown', function(e, editable) {
                    that.clickOnPermissions(that);
                });
                that.ui.addPermissionsSpan.click(function(e) {
                e.stopImmediatePropagation();
                that.$('a[data-js="permissions"]').editable('toggle');
                that.clickOnPermissions(that);
                });

        },

        clickOnPermissions : function(that) {
            var selectAll = true;
            var checklist = $('.editable-checklist').find('input[type="checkbox"]')
            _.each(checklist,function(checkbox){
                if($(checkbox).val() != -1 && !$(checkbox).is(':checked'))
                    selectAll = false;
            })
            if(selectAll){
                $('.editable-checklist').find('input[type="checkbox"][value="-1"]').prop('checked',true)
            } else {
                $('.editable-checklist').find('input[type="checkbox"][value="-1"]').prop('checked',false)
            }
            //for selectAll functionality
                        $('input[type="checkbox"][value="-1"]').click(function(e){
                var checkboxlist =$(this).closest('.editable-checklist').find('input[type="checkbox"][value!=-1]')
                $(this).is(':checked') ? checkboxlist.prop('checked',true) : checkboxlist.prop('checked',false);
            });
            $('.editable-checklist input[type="checkbox"]').click(function(e){
                if(this.value!="-1"){
                    var selectAll = true;
                    $('.editable-checklist input[type="checkbox"]').each(function(index,item){
                        if(item.value!="-1" && !item.checked){
                            selectAll = false;
                        }
                    });
                    $('input[type="checkbox"][value="-1"]').prop('checked',selectAll);
                }
            });
        },

        eGetResources :function () {
            var model = null;
            if (!_.isUndefined(this.model.get('resources'))) {
                this.model.set('policyType', 0);
                this.model.set('id', 'resource' + this.model.collection.length);
                model = $.extend(true, {}, this.model);
                // model = _.clone(resourceModel)
                // _.each(model.get('resources'), function(val, key, obj) {
                //     obj[key] = {
                //         'values': val
                //     };
                // });
            }
            // this.rangerServiceDefModel.get('resources')[0].lookupSupported = false
            if (_.isEmpty(this.serviceName)) {
                this.rangerServiceDefModel.get('resources').map( function(m){
                    m.lookupSupported = false;
                    m.validationRegEx = '';
                    m.validationMessage = '';
                })
            }
            this.form = new ServiceAuditFilterResourcesForm({
                template: require('hbs!tmpl/service/ServiceAuditFilterResourcesForm_tmpl'),
                model: model ? model : new RangerPolicyResource({
                    policyType: 0
                }),
                rangerServiceDefModel: this.rangerServiceDefModel,
                serviceName : this.serviceName,
            });
            this.modal = new Backbone.BootstrapModal({
                animate: true,
                content: this.form,
                title: 'Add/Edit Resources',
                okText: 'Save',
                allowCancel: true,
                escape: true
            }).open();
            this.modal.on("ok", this.onSave.bind(this, this.model));
        },

        onSave: function(resourceModel) {
            var that = this,
                tmpResource = {}, resource = {};
            var errors = this.form.commit({
                validate: false
            });
            if (!_.isEmpty(errors)) {
                that.modal.preventClose();
                return;
            }
            _.each(this.form.model.attributes, function(val, key) {
                if (key.indexOf("sameLevel") >= 0 && !_.isNull(val)) {
                    this.form.model.set(val.resourceType, val);
                    this.form.model.unset(key);
                }
            }, this);
            _.each(that.rangerServiceDefModel.get('resources'), function(obj) {
                var tmpObj = that.form.model.get(obj.name);
                if (!_.isUndefined(tmpObj) && _.isObject(tmpObj) && !_.isEmpty(tmpObj.resource)) {
                    tmpResource[obj.name] ={}, resource[obj.name] ={};
                    tmpResource[obj.name].values = tmpObj.resource.split(',');
                    resource[obj.name].values = tmpObj.resource.split(',');
                    if (obj.excludesSupported) {
                        resource[obj.name]['isExcludes'] = tmpObj.isExcludes;
                    }
                    if (obj.recursiveSupported) {
                        resource[obj.name]['isRecursive'] = tmpObj.isRecursive;
                    }
                    that.form.model.unset(obj.name);
                }
            });
            this.displayResources(resource);
            this.model.set('resources', resource)   ;
        },

        displayResources : function(tmpResource) {
            var $dataResources; 
            $dataResources = '<div class="resourceGrp"><div>'
                _.filter(tmpResource, function(key, value) {
                    var $toggleBtn =''
                    if(!_.isUndefined(key.isExcludes)) {
                        var isExcludes = key.isExcludes ? XAEnums.ExcludeStatus.STATUS_EXCLUDE.label : XAEnums.ExcludeStatus.STATUS_INCLUDE.label;
                        $toggleBtn += '<span class="badge badge-dark pull-right">'+isExcludes+'</span>'
                    }
                    if (!_.isUndefined(key.isRecursive)) {
                        var isRecursive = key.isRecursive ? XAEnums.RecursiveStatus.STATUS_RECURSIVE.label : XAEnums.RecursiveStatus.STATUS_NONRECURSIVE.label;
                        $toggleBtn += '<span class="badge badge-dark pull-right">'+isRecursive+'</span>'
                    }
                    $dataResources += '<div class="resourcesFilter"><div><b>' + value + '</b>:' + key.values.join(', ') +'</div>' + $toggleBtn +'</div>'
                })
            $dataResources += '</div>'
            this.$el.find('.js-formInput').html($dataResources);
            this.ui.addResources.find('i').attr('class', 'fa-fw fa fa-pencil');
        },

        eRemoveResources : function (argument) {
            var $dataResources = '<div class="resourceGrp text-center">--</div>'
            this.$el.find('.js-formInput').html($dataResources);
            if(this.model && !_.isUndefined(this.model.get('resources')))  {
                this.model.unset('resources');
            }
        },

        evSelectUserGroupRole : function (e) {
            e.currentTarget.select2(XAUtil.getUsersGroupsList(e.currentTarget.dataset.type, this))
        },
        evDelete : function(){
            var that = this;
            this.collection.remove(this.model);
        },
        selectOparations : function() {
            var that = this;
            var tags = [];
            this.ui.oparations.select2({
                multiple: true,
                closeOnSelect : true,
                placeholder : 'Select Action',
                allowClear: true,
                width : '200px',
                tokenSeparators: ["," , " "],
                tags : [],
                dropdownCssClass: 'select2-text-hidden',
                initSelection : function (element, callback) {
                    callback(tags);
                },
                createSearchChoice : function(term, data) {
                    if ($(data).filter(function() {
                        return this.text.localeCompare(term)===0; 
                    }).length===0)
                        {return {id:term, text:term};}
                },
            })
            if (this.model.get('actions')) {
                _.each(this.model.get('actions') , function(name){
                    tags.push( { 'id' : _.escape( name ), 'text' : _.escape( name ) } );
                });
                this.ui.oparations.select2('val',tags)
            }
        },

    });

    var AuditFilterList =  Backbone.Marionette.CompositeView.extend({
        _msvName : 'AuditFilterList',

        template : require('hbs!tmpl/service/AuditFilterList_tmpl'),

        templateHelpers :function(){
            return {
                emptyAuditFilterCol : this.collection.length == 0 ? true : false,
            }
        },
        
        getItemView : function(item){
            if(!item){
                return;
            }
            return AuditConfigItem;
        },
        itemViewContainer : ".js-formInput",

        itemViewOptions : function() {
            return {
                'collection' : this.collection,
                'accessTypes' : this.rangerServiceDefModel.get('accessTypes').filter(function(val) { return val !== null; }),
                'rangerServiceDefModel' : this.rangerServiceDefModel,
                'serviceName' : this.serviceName,
            };
        },
        events : {
            'click [data-action="addGroup"]' : 'addNew'
        },
        initialize : function(options) {
            _.extend(this, _.pick(options, 'rangerServiceDefModel', 'serviceName'));
            if(this.collection.length == 0){
                this.collection.add();
            }
        },
        onRender : function(){
            this.makePolicyItemSortable();
        },
        addNew : function(){
            var that =this;
            this.$el.find('.emptySet').remove();
            this.$('table').show();
            this.collection.add(new Backbone.Model());
        },
        makePolicyItemSortable : function(){
            var that = this, draggedModel;
            this.$el.find(".js-formInput" ).sortable({
                placeholder: "ui-state-highlight",
                start : function(event, ui){
                    var row = ui.item[0].rowIndex - 1;
                    draggedModel = that.collection.at(row);
                },
                stop : function(event, ui){
                    var row = ui.item[0].rowIndex -1;
                    that.collection.remove(draggedModel, { silent : true});
                    that.collection.add(draggedModel ,{ at: row, silent : true });
                    that.$el.find(ui.item[0]).addClass("dirtyField");
                },
            });
        }
    });
    
    return AuditFilterList;

});