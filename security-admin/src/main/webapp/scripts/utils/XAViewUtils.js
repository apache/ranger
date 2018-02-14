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

 

define(function(require) {
    'use strict';
	
    var XAEnums = require('utils/XAEnums');
    var localization = require('utils/XALangSupport');
    var XAViewUtil = {};

    XAViewUtil.resourceTypeFormatter = function(rawValue, model){
        var resourcePath = _.isUndefined(model.get('resourcePath')) ? undefined : model.get('resourcePath');
        var resourceType = _.isUndefined(model.get('resourceType')) ? undefined : model.get('resourceType');
        if(model.get('serviceType') == XAEnums.ServiceType.Service_HIVE.label && model.get('aclEnforcer') == "ranger-acl"
            && model.get('requestData')){
            if(resourcePath && !_.isEmpty(model.get('requestData'))) {
                return '<div class="clearfix">\
                            <div class="pull-left resourceText" title="'+ resourcePath+'">'+resourcePath+'</div>\
                            <div class="pull-right">\
                                <i class="icon-table queryInfo" title="Query Info" data-id ="'+model.get('id')+'"data-name = "queryInfo"></i>\
                            </div>\
                        </div>\
                        <div title="'+resourceType+'" class="border-top-1">'+resourceType+'</div>';
            }else{
                return '<div class="clearfix">\
                            <div class="pull-left">--</div>\
                            <div class="pull-right">\
                                <i class="icon-table queryInfo" title="Query Info" data-id ="'+model.get('id')+'"data-name = "queryInfo"></i>\
                            </div>\
                        </div>';
            }
        }else{
            if(resourcePath){
                return '<div class ="resourceText" title="'+resourcePath+'">'+resourcePath+'</div>\
                        <div title="'+resourceType+'" class="border-top-1">'+resourceType+'</div>';
            }else{
                return '--';
            }
        }
    };

    XAViewUtil.showQueryPopup = function(model, that){
        if(model.get('serviceType') == XAEnums.ServiceType.Service_HIVE.label && model.get('aclEnforcer') == "ranger-acl"
            && model.get('requestData') && !_.isEmpty(model.get('requestData'))){
            var msg = '<div class="query-content">'+model.get('requestData')+'</div>';
            var $elements = that.$el.find('table [data-name = "queryInfo"][data-id = "'+model.id+'"]');
            $elements.popover({
                html: true,
                title:'<b> Query </b>'+
                '<button type="button"  id="queryInfoClose" class="close closeBtn" onclick="$(&quot;.queryInfo&quot;).popover(&quot;hide&quot;);">&times;</button>',
                content: msg,
                selector : true,
                container:'body',
                placement: 'top',
            }).on("click", function(e){
                e.stopPropagation();
                if($(e.target).data('toggle') !== 'popover' && $(e.target).parents('.popover.in').length === 0){
                    $('.queryInfo').not(this).popover('hide');
                }
            });
        }
    };
	
	return XAViewUtil;

});
