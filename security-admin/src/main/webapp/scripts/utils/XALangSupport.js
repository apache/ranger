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

 /**
 * 
 * Loads different languages as required.Uses Globalize plugin   
 */

define(['require','modules/Vent','globalize','modules/globalize/message/en'],function(require,vent){

    var localization ={};

    function setCulture(culture){
        if(typeof culture !== 'undefined'){
            localization.culture =culture;
        }else{
            localization.culture ="en";
        }
        Globalize.culture(localization.culture);
    }

	localization.setDefaultCulture = function(){
		setCulture();
	}
    localization.tt = function(label){
        var ret = label;
  
        var str = localization.localize(label, localization.culture);
        if(typeof str !== 'undefined'){
        	return str;
        }
        
        if(localization.culture !== 'en' ){
        	if(typeof localization.culture !== 'undefined')
        		ret = (typeof localization.localize(label,"en") === 'undefined') ? label : localization.localize(label,"en");
        	else{
        		 ret = localization.localize(label,"en");
        	}
        }
        return ret;
    },
    localization.localize = function(key , culture){
    	return localization.byString(Globalize.findClosestCulture( culture ).messages, key ) || Globalize.cultures[ "default" ].messages[ key ];
    }
    
    localization.byString = function(o, s) {
        s = s.replace(/\[(\w+)\]/g, '.$1'); // convert indexes to properties
        s = s.replace(/^\./, '');           // strip a leading dot
        var a = s.split('.');
        while (a.length) {
            var n = a.shift();
            if (n in o) {
                o = o[n];
            } else {
                return;
            }
        }
        return o;
    }
    
    localization.getMonthsAbbr = function(){
		return Globalize.culture().calendars.standard.months.namesAbbr;
    }

    localization.getDaysOfWeek = function(label){
		return Globalize.culture().calendars.standard.days.namesAbbr;
    }
    
    localization.formatDate = function(val,format){
    	if(!val) return "";
		var XAUtil = require('utils/XAUtils');
		var valDate = XAUtil.DBToDateObj(val);
    	return Globalize.format( valDate,format,localization.culture);
    }
    
	//window.localization = localization;
    return localization;
})
