$(document).ready(function() {
    // Smooth scrolling for internal links
    $('a[href^="#"]').not('[data-toggle="dropdown"]').on('click', function(e) {
      e.preventDefault();

      var target = $(this.hash);
      if (target.length === 0) {
        target = $('a[name="' + this.hash.substr(1) + '"]');
      }
      if (target.length === 0) {
        target = $('html');
      }

      $('html, body').animate({
        scrollTop: target.offset().top
      }, 500);
    });
  });