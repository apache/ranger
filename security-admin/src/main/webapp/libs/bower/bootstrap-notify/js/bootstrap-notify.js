/**
 * bootstrap-notify.js v1.0
 * --
  * Copyright 2012 Goodybag, Inc.
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

(function ($) {
  var Notification = function (element, options) {
    // Element collection
    this.$element = $(element);
    this.$note    = $('<div class="alert"></div>');
    this.options  = $.extend(true, {}, $.fn.notify.defaults, options);

    // Setup from options
    if(this.options.transition) {
      if(this.options.transition == 'fade')
        this.$note.addClass('in').addClass(this.options.transition);
      else
        this.$note.addClass(this.options.transition);
    } else
      this.$note.addClass('fade').addClass('in');

    if(this.options.type)
      this.$note.addClass('alert-' + this.options.type);
    else
      this.$note.addClass('alert-success');

    if(!this.options.message && this.$element.data("message") !== '') // dom text
      this.$note.html(this.$element.data("message"));
    else
      if(typeof this.options.message === 'object') {
        if(this.options.message.html)
          this.$note.html(this.options.message.html);
        else if(this.options.message.text)
          this.$note.text(this.options.message.text);
      } else
        this.$note.html(this.options.message);
    
    var style = this.options.type == 'error' ? 'color:#a94442' : 'color:#3c763d';  
    
    if(this.options.closable) {
      var link = $('<a class="close pull-right" style="'+style+';" href="#">&times;</a>');
      $(link).on('click', $.proxy(onClose, this));
      this.$note.prepend(link);
    }

    if(this.options.pausable) {
    	var pauseLink = $('<a class="pause pull-right pause-play-close" style="'+style+';" href="#"><i class="icon-pause"></i></a><a class="play pull-right pause-play-close" href="#" style="'+style+';display:none;"><i class="icon-play"></i></a>');
    	$(pauseLink).on('click', $.proxy(onPause, this));
    	this.$note.prepend(pauseLink);
    	
    }
    return this;
  };

  var onClose = function() {
    this.options.onClose();
    $(this.$note).remove();
    this.options.onClosed();
    return false;
  };

  var onPause = function() {
	  if(this.$note.find('.pause').is(':visible')){
		  clearInterval(this.clearNotifyInterval)
		  this.$note.find('.pause').hide()
		  this.$note.find('.play').show()
	  }else{
		  setFadeOut(this)
		  this.$note.find('.pause').show()
		  this.$note.find('.play').hide()
	  }
	  return false;
  };
  var setFadeOut = function(self){
	  var that = self;
	  self.clearNotifyInterval = setTimeout(function() {
		  that.$note.fadeOut('slow', $.proxy(that.onClose, that));    
	  }, self.options.fadeOut.delay || 7000);
  };
	  
  Notification.prototype.show = function () {
	var that = this;  
    if(this.options.fadeOut.enabled){
    	setFadeOut(this)
    }
//  this.$note.delay(this.options.fadeOut.delay || 3000).fadeOut('slow', $.proxy(onClose, this));
    this.$element.append(this.$note);
    this.$note.alert();
  };

  Notification.prototype.hide = function () {
    if(this.options.fadeOut.enabled)
      this.$note.delay(this.options.fadeOut.delay || 3000).fadeOut('slow', $.proxy(onClose, this));
    else onClose.call(this);
  };

  $.fn.notify = function (options) {
    return new Notification(this, options);
  };

  $.fn.notify.defaults = {
    type: 'success',
    closable: true,
    transition: 'fade',
    fadeOut: {
      enabled: true,
      delay: 7000
    },
    message: null,
    onClose: function () {},
    onClosed: function () {},
    pausable: false
  }
})(window.jQuery);
