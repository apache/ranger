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
    'use strict';

    var Backbone            = require('backbone');
    var XALinks             = require('modules/XALinks');
    var XAEnums             = require('utils/XAEnums');
    var XAUtil              = require('utils/XAUtils');
    var SessionMgr          = require('mgrs/SessionMgr');
    var App                 = require('App');
    var localization        = require('utils/XALangSupport');
    var RangerServiceList   = require('collections/RangerServiceList');
    var RangerService       = require('models/RangerService');
    var ServicemanagerSidebarlayoutTmpl = require('hbs!tmpl/common/ServiceManagerSidebarLayout_tmpl');
    var vUploadServicePolicy     = require('views/UploadServicePolicy');
    var vDownloadServicePolicy   = require('views/DownloadServicePolicy');
    var RangerServiceViewDetail  = require('views/service/RangerServiceViewDetail');
    var RangerServiceDefList    = require('collections/RangerServiceDefList');
    var RangerServiceDef        = require('models/RangerServiceDef');
    var RangerZoneList      = require('collections/RangerZoneList');

    require('Backbone.BootstrapModal');
    return Backbone.Marionette.Layout.extend(
    /** @lends Servicemanagerlayout */
    {
        _viewName : name,

        template: ServicemanagerSidebarlayoutTmpl,

        templateHelpers: function(){
            return {
                operation   : SessionMgr.isSystemAdmin() || SessionMgr.isKeyAdmin(),
                serviceDefs : _.sortBy(this.componentCollectionModels(App.vZone.vZoneName), function(m) {return m.get('name')}),
                services    : this.componentServicesModels(App.vZone.vZoneName),
                showImportExportBtn : (SessionMgr.isUser() || XAUtil.isAuditorOrKMSAuditor(SessionMgr)) ? false : true,
                isZoneAdministration : (SessionMgr.isSystemAdmin()|| SessionMgr.isUser() || SessionMgr.isAuditor()) ? true : false,
                isServiceManager : (App.vZone && _.isEmpty(App.vZone.vZoneName)) ? true : false,
                isZoneCreateAdministration : (SessionMgr.isSystemAdmin()) ? true : false,
                showPermissionTab : XAUtil.isAuditorOrSystemAdmin(SessionMgr),
               // viewManager : this.viewManager,
            };
        },

        breadCrumbs :function(){
            if(this.type == "tag"){
                if(App.vZone && App.vZone.vZoneName && !_.isEmpty(App.vZone.vZoneName)){
                    return [XALinks.get('TagBasedServiceManager', App.vZone.vZoneName)];
                }else{
                    return [XALinks.get('TagBasedServiceManager')];
                }
            }else{
                if(App.vZone && App.vZone.vZoneName && !_.isEmpty(App.vZone.vZoneName)){
                    return [XALinks.get('ServiceManager', App.vZone.vZoneName)];
                }else{
                    return [XALinks.get('ServiceManager')];
                }
            }
        },

        /** Layout sub regions */
        regions: {
            'rServiceManager' : 'div[data-id="serviceManager"]'
        },

        /** ui selector cache */
        ui: {
            'btnDelete' : '.deleteRepo',
            'downloadReport'      : '[data-id="downloadBtnOnService"]',
            'uploadServiceReport' :'[data-id="uploadBtnOnServices"]',
            'exportReport'      : '[data-id="exportBtn"]',
            'importServiceReport' :'[data-id="importBtn"]',
            'viewServices' : '[data-name="viewService"]',
            'selectZoneName' : '[data-id="selectZoneName"]',
            'resource' : '[data-js="resource"]',
            'tag' : '[data-js="tag"]',
            'panel' : '[data-id="panel"] i#collapesService',
            'serviceActive' : '.serviceActive a',
            'zoneSearch' : '[data-id="zoneSearch"]',
            'zoneUlList' : '[data-id="zoneUlList"]',
            'sideCollapes' : '[data-id="sideCollapes"]',
            'selectComponet' : '[data-id="selectComponentName"]',
            'viewManager' : '.viewManager',
            'expandCollapes' : '[data-id="collapesAll"]',
            'policyName'          : '[data-js="policyName"]',
            'componentType'       : '[data-id="component"]',
            'policyType'          : '[data-id="policyType"]',
            'policyLabels'        : '[data-id="policyLabels"]',
            'zoneName'            : '[data-id="zoneName"]',
            'selectUserGroup'     : '[data-id="btnUserGroup"]',
            'resourceName'        : '[data-js="resourceName"]',
            'userGroup'           : '[data-js="selectGroups"]',
            'searchBtn'           : '[data-js="searchBtn"]',
            'userName'            : '[data-js="userName"]',
            'selectServiceName'   : '[data-js="serviceName"]',
            'profileTab'          : '.profile-tab'

        },

        /** ui events hash */
        events : function(){
            var events = {};
            events['click ' + this.ui.downloadReport]   = 'downloadReport';
            events['click ' + this.ui.uploadServiceReport]  = 'uploadServiceReport';
            events['click ' + this.ui.exportReport] = 'downloadReport';
            events['click ' + this.ui.importServiceReport]  = 'uploadServiceReport';
            events['click ' + this.ui.selectZoneName]   = 'selectZoneName';
            //events['click ' + this.ui.componetList]   = 'componetList';
            events['click ' + this.ui.panel]   = 'onPanelToggle';
            events['click ' + this.ui.serviceActive]   = 'serviceActive';
            events['keyup ' + this.ui.zoneSearch] = 'zoneSearch';
            events['click ' + this.ui.sideCollapes] = 'sideCollapes';
            events['click ' + this.ui.selectComponet] = 'selectComponet';
            events['click ' + this.ui.expandCollapes] = 'expandCollapes';
            events['click .autoText']  = 'autocompleteFilter';
            events['click ' + this.ui.searchBtn]  = 'onSearch';
            events['click ' + this.ui.profileTab +' li a']  = 'onProfileTab';

            return events;
        },
        /**
        * intialize a new Servicemanagerlayout Layout
        * @constructs
        */
        initialize: function(options) {
            console.log("initialized a ServiceLayoutSidebar Layout");
             _.extend(this, _.pick(options ,'type'));
            this.collapes = false, this.selectedComponets = [];
            this.componentListing(this.type);
            this.bindEvents();
           // this.initializeServices();
            if (!App.vZone) {
                App.vZone = {
                    vZoneName: ""
                }
            }
            if (!_.isUndefined(XAUtil.urlQueryParams())) {
                var searchFregment = XAUtil.changeUrlToSearchQuery(decodeURIComponent(XAUtil.urlQueryParams()));
                if(_.has(searchFregment, 'securityZone')) {
                    App.vZone.vZoneName = searchFregment['securityZone'];
                }
            }
            this.initialCall = true;
        },

        /** all events binding here */
        bindEvents : function(){
            /*this.listenTo(communicator.vent,'someView:someEvent', this.someEventHandler, this)'*/
            this.listenTo(this.collection, "sync", this.render, this);
            this.listenTo(this.collection, "request", function(){
                this.$('[data-id="r_tableSpinner"]').removeClass('display-none').addClass('loading');
            }, this);
        },

        sideCollapes : function (e) {
            if (this.collapes) {
                this.collapes = false;
                App.rSideBar.$el.addClass('expanded');
                App.rSideBar.$el.removeClass('collapsed');
                //$(e.target).toggleClass('icon-double-angle-left icon-2x');
                e.target.setAttribute('class' , 'icon-double-angle-left icon-2x')
            } else {
                this.collapes = true;
                App.rSideBar.$el.addClass('collapsed');
                App.rSideBar.$el.removeClass('expanded');
                //$(e.target).toggleClass('icon-double-angle-right icon-2x');
                e.target.setAttribute('class' , 'icon-double-angle-right icon-2x');
            }
        },

        onPanelToggle : function (e) {
            $(e.currentTarget).toggleClass('icon-caret-down');
            $(e.currentTarget).parent().next().slideToggle();
        },

        expandCollapes : function(e) {
            console.log(e);
            e.stopPropagation();
            this.$el.find('[data-id="panel"] i#collapesService').each(function(){
                $(this).toggleClass('icon-caret-down');
                $(this).parent().next().slideToggle();
            })
        },

        /** on render callback */
        onRender: function() {
            var that = this;
            this.$('[data-id="r_tableSpinner"]').removeClass('loading').addClass('display-none');
            if (this.rangerZoneList.length > 0) {
                this.ui.selectZoneName.removeAttr('disabled');
                this.$el.find('.zoneEmptyMsg').removeAttr('title');
            }
            this.selectZoneName();
            this.selectComponet();
            if(this.type == "resource") {
                this.ui.resource.addClass("btn-primary");
                this.ui.tag.removeClass("btn-primary");
            } else {
                this.ui.resource.removeClass("btn-primary");
                this.ui.tag.addClass("btn-primary");
            }
            this.setupZoneList(this.rangerZoneList.models);
            // if(this.selectedService) {
            //     this.ui.serviceActive.each(function() {
            //         if($(this).data('id') == that.selectedService) {
            //             $(this).parent().addClass('selectedList');
            //         }
            //     })
            // }
        },
        /** all post render plugin initialization */
        initializePlugins: function(){
            if(this.ui.serviceActive.length > 0) {
                this.ui.serviceActive[0].click()
            }
        },

        selecttedService : function(serviceId){
            this.ui.serviceActive.parent().removeClass('selectedList')
            this.ui.serviceActive.each(function() {
                if($(this).data('id') == serviceId) {
                    $(this).parent().addClass('selectedList');
                }
            })
        },

        componentListing: function(type) {
            this.collection = new RangerServiceDefList();
            this.collection.queryParams.sortBy = 'serviceTypeId';
            if(type == 'tag'){
                var tagServiceDef    = new RangerServiceDef();
                tagServiceDef.url    = XAUtil.getRangerServiceDef(XAEnums.ServiceType.SERVICE_TAG.label)
                tagServiceDef.fetch({
                    cache : false,
                    async:false
                })
                this.collection.add(tagServiceDef);
            }else{
                this.collection.fetch({
                    cache : false,
                    async:false
                });
                var coll = this.collection.filter(function(model){ return model.get('name') != XAEnums.ServiceType.SERVICE_TAG.label})
                this.collection.reset(coll)
            }
        },

        zoneCollection : function() {
            this.rangerZoneList = new RangerZoneList();
            this.rangerZoneList.fetch({
                cache : false,
                async : false,
            })
        },

        initializeServices : function(){
            this.services = new RangerServiceList();
            this.services.setPageSize(100);
            this.services.fetch({
               cache : false,
               async : false
            });

        },
        downloadReport : function(e){
            var that = this;
            if(SessionMgr.isKeyAdmin()){
                if(this.services.length == 0){
                    XAUtil.alertBoxWithTimeSet(localization.tt('msg.noServiceToExport'));
                    return;
                }
            }
            var el = $(e.currentTarget), serviceType = el.attr('data-servicetype');
            if(serviceType){
                var componentServices = this.services.where({'type' : serviceType });
                    if(componentServices.length == 0 ){
                    XAUtil.alertBoxWithTimeSet(localization.tt('msg.noServiceToExport'));
                    return;
                }
            }else{
                if(SessionMgr.isSystemAdmin()){
                    if(location.hash == "#!/policymanager/resource"){
                        var servicesList = _.omit(this.services.groupBy('type'),'tag','kms');
                        if(_.isEmpty(servicesList)){
                            XAUtil.alertBoxWithTimeSet(localization.tt('msg.noServiceToExport'));
                            return;
                        }
                    }else{
                        var servicesList = _.pick(this.services.groupBy('type'),'tag');
                        if(_.isEmpty(servicesList)){
                            XAUtil.alertBoxWithTimeSet(localization.tt('msg.noServiceToExport'));
                            return;
                        }
                    }
                }
            }
            var view = new vDownloadServicePolicy({
                serviceType     :serviceType,
                collection      : new Backbone.Collection([""]),
                serviceDefList  : this.collection,
                                services        : this.services,
                zoneServiceDefList : this.componentCollectionModels(this.ui.selectZoneName.val()),
                zoneServices    : this.componentServicesModels(this.ui.selectZoneName.val()),

            });
            var modal = new Backbone.BootstrapModal({
                content : view,
                title   : 'Export Policy',
                okText  :"Export",
                animate : true
            }).open();
        },
        uploadServiceReport :function(e){
            var that = this;
            if(SessionMgr.isKeyAdmin()){
                if(this.services.length == 0){
                    XAUtil.alertBoxWithTimeSet(localization.tt('msg.noServiceToImport'));
                    return;
                }
            }
            var el = $(e.currentTarget), serviceType = el.attr('data-servicetype');
            if(serviceType){
                var componentServices = this.services.where({'type' : serviceType });
                    if(componentServices.length == 0 ){
                    XAUtil.alertBoxWithTimeSet(localization.tt('msg.noServiceToImport'));
                    return;
                }
            }else{
                if(SessionMgr.isSystemAdmin()){
                    if(location.hash=="#!/policymanager/resource"){
                        var servicesList = _.omit(this.services.groupBy('type'),'tag','kms')
                        if(_.isEmpty(servicesList)){
                            XAUtil.alertBoxWithTimeSet(localization.tt('msg.noServiceToImport'));
                            return;
                        }
                    }else{
                        var servicesList = _.pick(this.services.groupBy('type'),'tag')
                        if(_.isEmpty(servicesList)){
                            XAUtil.alertBoxWithTimeSet(localization.tt('msg.noServiceToImport'));
                            return;
                        }
                        
                    }
                }
            }
            var view = new vUploadServicePolicy({
                serviceType     : serviceType,
                collection      : new Backbone.Collection(),
                serviceDefList  : this.collection,
                services        : this.services,
                rangerZoneList  : this.rangerZoneList,
            });
            var modal = new Backbone.BootstrapModal({
                content : view,
                okText  :"Import",
                                title   : App.vZone && App.vZone.vZoneName && !_.isEmpty(App.vZone.vZoneName) ? 'Import Policy For Zone' : 'Import Policy',
                animate : true
            }).open();

        },

        selectZoneName : function(){
            var that = this;
            var zoneName = _.map(this.rangerZoneList.models, function(m){
                return { 'id':m.get('name'), 'text':m.get('name'), 'zoneId' : m.get('id')}
            });
            if(!_.isEmpty(App.vZone.vZoneName) && !_.isUndefined(App.vZone.vZoneName)){
                this.ui.selectZoneName.val(App.vZone.vZoneName);
            }
            var servicesModel = _.clone(that.services);
            this.ui.selectZoneName.select2({
                closeOnSelect: false,
                maximumSelectionSize : 1,
                width: '270px',
                allowClear: true,
                data: zoneName,
                placeholder: 'Select Security Zones',
            }).on('change', function(e){
                App.vZone.vZoneName = e.val;
                if(e.added){
                    App.vZone.vZoneId = e.added.zoneId;
                    XAUtil.changeParamToUrlFragment({"securityZone" : e.val}, that.collection.modelName);
                } else {
                    App.vZone.vZoneId = null;
                    //for url change on UI
                    XAUtil.changeParamToUrlFragment();
                }
                var rBreadcrumbsText = !_.isEmpty(App.vZone.vZoneName) ? 'Service Manager : ' + App.vZone.vZoneName + ' zone' : 'Service Manager';
                App.rBreadcrumbs.currentView.breadcrumb[0].text = rBreadcrumbsText;
                App.rBreadcrumbs.currentView.render()
                that.selectedComponets =[];
                that.render();
                that.initializePlugins();
                if(that.type == "tag") {
                    App.appRouter.navigate("#!/policymanager/tag",{trigger: true});
                } else {
                    App.appRouter.navigate("#!/policymanager/resource",{trigger: true});
                }
                that.ui.selectZoneName.select2('val', e.val);
            });
        },

        selectComponet : function(){
            var that = this, options;
            if(!_.isEmpty(App.vZone.vZoneName) && !_.isUndefined(App.vZone.vZoneName)) {
                var serviceType = _.keys(that.componentServicesModels(App.vZone.vZoneName));
                options = serviceType.map(function(m){ return { 'id' : m, 'text' : m.toUpperCase()}})
            } else {
                options = this.collection.map(function(m){ return { 'id' : (m.get('name')), 'text' : (m.get('name')).toUpperCase()}});
            }
            if(!_.isEmpty(that.selectedComponets)) {
                this.ui.selectComponet.val(that.selectedComponets);
            }
            this.ui.selectComponet.select2({
                multiple: true,
                closeOnSelect: true,
                placeholder: ' Select Service Types',
                //maximumSelectionSize : 1,
                width: '270px',
                allowClear: true,
                data: _.sortBy(options, 'text')
            }).on('change', function(e){
                console.log(e);
                that.selectedComponets = e.val;
                that.render();
                that.initializePlugins();
            });
        },

        componentCollectionModels: function(zoneName) {
            var that = this;
            if (!_.isEmpty(zoneName) && !_.isUndefined(zoneName) && this.type !== XAEnums.ServiceType.SERVICE_TAG.label) {
                var serviceType = _.keys(that.componentServicesModels(zoneName));
                if(!_.isEmpty(that.selectedComponets)) {
                    serviceType = _.intersection(serviceType,that.selectedComponets);
                }
                return that.collection.filter(function(model) {
                    return serviceType.indexOf(model.get("name")) !== -1;
                })
            } else {
                if(!_.isEmpty(that.selectedComponets)) {
                    return that.collection.filter(function(model) {
                        return that.selectedComponets.indexOf(model.get("name")) !== -1;
                    })
                } else {
                    return that.collection.models;
                }
            }
        },

        componentServicesModels: function(zoneName) {
            var that = this;
            this.initializeServices();
            this.zoneCollection();
            if(!_.isEmpty(zoneName) && !_.isUndefined(zoneName) && that.rangerZoneList.length > 0){
                var selectedZone = that.rangerZoneList.find(function(m) {
                    return zoneName === m.get('name');
                });
            }
            if (selectedZone && !_.isEmpty(selectedZone)) {
                var selectedZoneServices = [], model;
                if(this.type !== XAEnums.ServiceType.SERVICE_TAG.label){
                    _.each(selectedZone.get('services'), function(value, key) {
                        model = that.services.find(function(m) {
                            return m.get('name') == key
                        });
                        if (model) {
                            selectedZoneServices.push(model);
                        }
                    });
                }else{
                    _.each(selectedZone.get('tagServices'), function(value){
                        model = that.services.find(function(m) {
                            return m.get('name') == value
                        });
                        if (model) {
                            selectedZoneServices.push(model);
                        }
                    })
                }
                return _.groupBy(selectedZoneServices, function(m) {
                        return m.get('type')
                });
            } else {
                return that.services.groupBy("type")
            }
        },

        componetList: function(type) {
            if(type == this.type) return false
            if(type == "resource") {
                this.type = "resource";
            } else {
                this.type = "tag";
            }
            this.componentListing(this.type);
            this.render();
        },

        // serviceActive: function (e) {
        //     this.ui.serviceActive.parent().removeClass('selectedList')
        //     e.stopPropagation();
        //     $(e.currentTarget).parent().addClass('selectedList');
        //     this.selectedService = e.currentTarget.dataset.id
        // },

        selectedList: function(target) {
            console.log(target);
            this.ui.viewManager.find('.selected').removeClass('selected')
            this.ui.viewManager.find('[data-id="'+target+'"]').addClass('selected')

        },

        zoneSearch: function() {
            var input = this.ui.zoneSearch.val();
            var that = this;
            that.zoneSearchList = [];

            if (!_.isEmpty(input)) {
                that.zoneSearchList = this.rangerZoneList.filter(
                    function(zone) {
                        return (zone.get('name').toLowerCase().indexOf(input.toLowerCase()) > -1)
                    }
                );
                this.setupZoneList(that.zoneSearchList);
            } else {
                this.setupZoneList(this.rangerZoneList.models);
            }
        },

        setupZoneList: function(zoneArray) {
            var that = this;
            this.ui.zoneUlList.empty();
            if(zoneArray.length > 0) {
                _.each(zoneArray,
                    function(zone) {
                        if(that.rangerZoneList.models[0].get('name') == zone.get('name')) {
                            that.ui.zoneUlList.append('<li class="trim-containt" title="'+_.escape(zone.get('name'))+
                                '" data-action="zoneListing" data-id="' + _.escape(zone.get('name')) + '"><a href="#!/zones/zone/'+zone.get('id')+'">' + _.escape(zone.get('name')) + '</a></li>');
                        } else {
                            that.ui.zoneUlList.append('<li class="trim-containt" data-action="zoneListing" title="'
                                +_.escape(zone.get('name'))+'" data-id="' + _.escape(zone.get('name')) + '"><a href="#!/zones/zone/'+zone.get('id')+'">' + _.escape(zone.get('name')) + '</a></li>');
                        }
                    }
                );
            } else {
                this.ui.zoneUlList.append('<li><h4>No Zone Found !</li>');
            }
        },

        autocompleteFilter : function (e) {
            App.rContent.currentView.autocompleteFilter(e)
        },

        onSearch : function (e) {
            App.rContent.currentView.onSearch(e)
        },

        onProfileTab :function (e) {
            this.ui.profileTab.find('.selected').removeClass('selected')
            this.ui.profileTab.find(e.currentTarget.parentElement).addClass('selected')
            App.rContent.currentView.onTabChange(e)

        },

        /** on close */
        onClose: function(){
            XAUtil.removeUnwantedDomElement();
        }

    });
});
