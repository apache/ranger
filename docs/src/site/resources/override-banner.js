// Using URL
const currentPageUrl = window.location.href;
 // Check if the current page is the introduction page
const isIntroductionPage = currentPageUrl.includes("index") || currentPageUrl.endsWith("/");

// Get the banner, main-body and hide element
const bannerLeft = document.getElementById("bannerLeft");
const mainBody = document.getElementsByClassName("main-body")[0];
const hide = document.getElementsByClassName("hide")[0];

// Hide the banner element based on the introduction page
if (isIntroductionPage) {
    hide.classList.remove("hide");
    mainBody.style.paddingTop = "0";
}
