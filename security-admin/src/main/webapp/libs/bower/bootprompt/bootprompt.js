/**
     * bootprompt.js
     * license: MIT
     * http://github.com/lddubeau/bootprompt
     * bootprompt@6.0.2
*/


(function (global, factory) {
    typeof exports === 'object' && typeof module !== 'undefined' ? factory(exports, require('bootstrap'), require('jquery')) :
    typeof define === 'function' && define.amd ? define(['exports', 'bootstrap', 'jquery'], factory) :
    (global = global || self, factory(global.bootprompt = {}, null, global.$));
}(this, function (exports, bootstrap, $) { 'use strict';

    $ = $ && $.hasOwnProperty('default') ? $['default'] : $;

    var __assign = (undefined && undefined.__assign) || function () {
        __assign = Object.assign || function(t) {
            for (var s, i = 1, n = arguments.length; i < n; i++) {
                s = arguments[i];
                for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p))
                    t[p] = s[p];
            }
            return t;
        };
        return __assign.apply(this, arguments);
    };
    var __awaiter = (undefined && undefined.__awaiter) || function (thisArg, _arguments, P, generator) {
        return new (P || (P = Promise))(function (resolve, reject) {
            function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
            function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
            function step(result) { result.done ? resolve(result.value) : new P(function (resolve) { resolve(result.value); }).then(fulfilled, rejected); }
            step((generator = generator.apply(thisArg, _arguments || [])).next());
        });
    };
    var __generator = (undefined && undefined.__generator) || function (thisArg, body) {
        var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
        return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
        function verb(n) { return function (v) { return step([n, v]); }; }
        function step(op) {
            if (f) throw new TypeError("Generator is already executing.");
            while (_) try {
                if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
                if (y = 0, t) op = [op[0] & 2, t.value];
                switch (op[0]) {
                    case 0: case 1: t = op; break;
                    case 4: _.label++; return { value: op[1], done: false };
                    case 5: _.label++; y = op[1]; op = [0]; continue;
                    case 7: op = _.ops.pop(); _.trys.pop(); continue;
                    default:
                        if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                        if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                        if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                        if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                        if (t[2]) _.ops.pop();
                        _.trys.pop(); continue;
                }
                op = body.call(thisArg, _);
            } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
            if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
        }
    };
    var __values = (undefined && undefined.__values) || function (o) {
        var m = typeof Symbol === "function" && o[Symbol.iterator], i = 0;
        if (m) return m.call(o);
        return {
            next: function () {
                if (o && i >= o.length) o = void 0;
                return { value: o && o[i++], done: !o };
            }
        };
    };
    // We use this to keep versync happy.
    var version = "6.0.2";
    // But export this.
    /** Bootprompt's version */
    var VERSION = version;
    var LOCALE_FIELDS = ["OK", "CANCEL", "CONFIRM"];
    var definedLocales = Object.create(null);
    var templates = {
        dialog: "<div class=\"bootprompt modal\" tabindex=\"-1\" role=\"dialog\" aria-hidden=\"true\"> <div class=\"modal-dialog\">  <div class=\"modal-content\">   <div class=\"modal-body\"><div class=\"bootprompt-body\"></div></div>  </div> </div></div>",
        header: "<div class=\"modal-header\"> <h5 class=\"modal-title\"></h5></div>",
        footer: "<div class=\"modal-footer\"></div>",
        closeButton: "\n<button type=\"button\" class=\"bootprompt-close-button close\" aria-hidden=\"true\">&times;</button>",
        form: "<form class=\"bootprompt-form\"></form>",
        button: "<button type=\"button\" class=\"btn\"></button>",
        option: "<option></option>",
        promptMessage: "<div class=\"bootprompt-prompt-message\"></div>",
        inputs: {
            text: "<input class=\"bootprompt-input bootprompt-input-text form-control\" autocomplete=\"off\" type=\"text\" />",
            textarea: "<textarea class=\"bootprompt-input bootprompt-input-textarea form-control\"></textarea>",
            email: "<input class=\"bootprompt-input bootprompt-input-email form-control\" autocomplete=\"off\" type=\"email\" />",
            select: "<select class=\"bootprompt-input bootprompt-input-select form-control\"></select>",
            checkbox: "<div class=\"form-check checkbox\"><label class=\"form-check-label\"> <input class=\"form-check-input bootprompt-input bootprompt-input-checkbox\"\n        type=\"checkbox\" /></label></div>",
            radio: "<div class=\"form-check radio\"><label class=\"form-check-label\"><input class=\"form-check-input bootprompt-input bootprompt-input-radio\"        type=\"radio\" name=\"bootprompt-radio\" /></label></div>",
            date: "<input class=\"bootprompt-input bootprompt-input-date form-control\" autocomplete=\"off\" type=\"date\" />",
            time: "<input class=\"bootprompt-input bootprompt-input-time form-control\" autocomplete=\"off\" type=\"time\" />",
            number: "<input class=\"bootprompt-input bootprompt-input-number form-control\"        autocomplete=\"off\" type=\"number\" />",
            password: "<input class=\"bootprompt-input bootprompt-input-password form-control\" autocomplete=\"off\" type=\"password\" />",
            range: "<input class=\"bootprompt-input bootprompt-input-range form-control-range\"\nautocomplete=\"off\" type=\"range\" />",
        },
    };
    var currentLocale = "en";
    var animate = true;
    function locales(name) {
        return name !== undefined ? definedLocales[name] : definedLocales;
    }
    /**
     * Register a locale.
     *
     * @param name The name of the locale.
     *
     * @param values The locale specification, which determines how to translate
     * each field.
     *
     * @throws {Error} If a field is missing from ``values``.
     */
    function addLocale(name, values) {
        var e_1, _a;
        try {
            for (var LOCALE_FIELDS_1 = __values(LOCALE_FIELDS), LOCALE_FIELDS_1_1 = LOCALE_FIELDS_1.next(); !LOCALE_FIELDS_1_1.done; LOCALE_FIELDS_1_1 = LOCALE_FIELDS_1.next()) {
                var field = LOCALE_FIELDS_1_1.value;
                if (typeof values[field] !== "string") {
                    throw new Error("Please supply a translation for \"" + field + "\"");
                }
            }
        }
        catch (e_1_1) { e_1 = { error: e_1_1 }; }
        finally {
            try {
                if (LOCALE_FIELDS_1_1 && !LOCALE_FIELDS_1_1.done && (_a = LOCALE_FIELDS_1.return)) _a.call(LOCALE_FIELDS_1);
            }
            finally { if (e_1) throw e_1.error; }
        }
        definedLocales[name] = values;
    }
    /**
     * Remove a locale. Removing an unknown locale is a no-op.
     *
     * @param name The name of the locale.
     *
     * @throws {Error} If ``name`` is ``"en"``. This locale cannot be removed.
     */
    function removeLocale(name) {
        if (name !== "en") {
            delete definedLocales[name];
        }
        else {
            throw new Error("\"en\" is used as the default and fallback locale and cannot be removed.");
        }
    }
    /**
     * Set the default locale. Note that this function does not check whether the
     * locale is known.
     */
    function setLocale(name) {
        currentLocale = name;
    }
    /**
     * Set the ``animate`` flag. When the flag is on, the modals produced by this
     * library are animated. When off, they are not animated.
     *
     * **NOTE**: The reason this function exists is to be able to turn off
     * animations during testing. Having the animations turned on can turn simple
     * tests into complicated afairs because it takes a while for a modal to show
     * up or be removed. We do not recommend using this function in production.
     */
    function setAnimate(value) {
        animate = value;
    }
    /**
     * Hide all modals created with bootprompt.
     */
    function hideAll() {
        $(".bootprompt").modal("hide");
    }
    //
    // CORE HELPER FUNCTIONS
    //
    // tslint:disable-next-line:no-any
    var fnModal = $.fn.modal;
    /* istanbul ignore if: we do not test with incorrect environments. */
    if (fnModal === undefined) {
        throw new Error("\"$.fn.modal\" is not defined; please double check you have included the Bootstrap JavaScript library. See http://getbootstrap.com/javascript/ for more details.");
    }
    /* istanbul ignore if: we do not test with incorrect environments. */
    if (!fnModal.Constructor.VERSION) {
        throw new Error("Bootprompt cannot determine the version of Bootstrap used");
    }
    var fullBootstrapVersion = fnModal.Constructor.VERSION;
    var bootstrapVersion = Number(fullBootstrapVersion.substring(0, fullBootstrapVersion.indexOf(".")));
    /* istanbul ignore if: we do not test with incorrect environments. */
    if (bootstrapVersion < 3) {
        throw new Error("Bootprompt does not work with Bootstrap 2 and lower.");
    }
    /**
     * This is a general-purpose function allowing to create custom dialogs.
     *
     * @param options The options that govern how the dialog is created.
     *
     * @returns The jQuery object which models the dialog.
     */
    // tslint:disable-next-line:max-func-body-length cyclomatic-complexity
    function dialog(options) {
        var e_2, _a, e_3, _b;
        var finalOptions = sanitize(options);
        var $modal = $(templates.dialog);
        var modal = $modal[0];
        var innerDialog = modal.getElementsByClassName("modal-dialog")[0];
        var body = modal.getElementsByClassName("modal-body")[0];
        var footer = $(templates.footer)[0];
        var callbacks = {
            onEscape: finalOptions.onEscape,
            onClose: finalOptions.onClose,
        };
        if (callbacks.onEscape === undefined) {
            callbacks.onEscape = true;
        }
        var buttons = finalOptions.buttons, backdrop = finalOptions.backdrop, className = finalOptions.className, closeButton = finalOptions.closeButton, message = finalOptions.message, size = finalOptions.size, title = finalOptions.title;
        // tslint:disable-next-line:no-non-null-assertion
        var bpBody = body.getElementsByClassName("bootprompt-body")[0];
        if (typeof message === "string") {
            // tslint:disable-next-line:no-inner-html
            bpBody.innerHTML = message;
        }
        else {
            // tslint:disable-next-line:no-inner-html
            bpBody.innerHTML = "";
            $(bpBody).append(message);
        }
        var hadButtons = false;
        // tslint:disable-next-line:forin
        for (var key in buttons) {
            hadButtons = true;
            var b = buttons[key];
            var $button = $(templates.button);
            var button = $button[0];
            $button.data("bp-handler", key);
            try {
                // On IE10/11 it is not possible to just do x.classList.add(a, b, c).
                for (var _c = __values(b.className.split(" ")), _d = _c.next(); !_d.done; _d = _c.next()) {
                    var cl = _d.value;
                    button.classList.add(cl);
                }
            }
            catch (e_2_1) { e_2 = { error: e_2_1 }; }
            finally {
                try {
                    if (_d && !_d.done && (_a = _c.return)) _a.call(_c);
                }
                finally { if (e_2) throw e_2.error; }
            }
            switch (key) {
                case "ok":
                case "confirm":
                    button.classList.add("bootprompt-accept");
                    break;
                case "cancel":
                    button.classList.add("bootprompt-cancel");
                    break;
                default:
            }
            // tslint:disable-next-line:no-inner-html
            button.innerHTML = b.label;
            footer.appendChild(button);
            callbacks[key] = b.callback;
        }
        // Only attempt to create buttons if at least one has been defined in the
        // options object.
        if (hadButtons) {
            // tslint:disable-next-line:no-non-null-assertion
            body.parentNode.insertBefore(footer, body.nextSibling);
        }
        if (finalOptions.animate === true) {
            modal.classList.add("fade");
        }
        if (className !== undefined) {
            try {
                // On IE10/11 it is not possible to just do x.classList.add(a, b, c).
                for (var _e = __values(className.split(" ")), _f = _e.next(); !_f.done; _f = _e.next()) {
                    var cl = _f.value;
                    modal.classList.add(cl);
                }
            }
            catch (e_3_1) { e_3 = { error: e_3_1 }; }
            finally {
                try {
                    if (_f && !_f.done && (_b = _e.return)) _b.call(_e);
                }
                finally { if (e_3) throw e_3.error; }
            }
        }
        if (size !== undefined) {
            // Requires Bootstrap 3.1.0 or higher
            /* istanbul ignore if: we don't systematically test with old versions */
            if (fullBootstrapVersion.substring(0, 3) < "3.1") {
                // tslint:disable-next-line:no-console
                console.warn("\"size\" requires Bootstrap 3.1.0 or higher. You appear to be using " + fullBootstrapVersion + ". Please upgrade to use this option.");
            }
            switch (size) {
                case "large":
                    innerDialog.classList.add("modal-lg");
                    break;
                case "small":
                    innerDialog.classList.add("modal-sm");
                    break;
                default:
                    var q = size;
                    throw new Error("unknown size value: " + q);
            }
        }
        if (title !== undefined) {
            // tslint:disable-next-line:no-non-null-assertion
            body.parentNode.insertBefore($(templates.header)[0], body);
            var modalTitle = modal.getElementsByClassName("modal-title")[0];
            if (typeof title === "string") {
                // tslint:disable-next-line:no-inner-html
                modalTitle.innerHTML = title;
            }
            else {
                // tslint:disable-next-line:no-inner-html
                modalTitle.innerHTML = "";
                $(modalTitle).append(title);
            }
        }
        if (closeButton === true) {
            var closeButtonEl = $(templates.closeButton)[0];
            if (title !== undefined) {
                var modalHeader = modal.getElementsByClassName("modal-header")[0];
                /* istanbul ignore else: we don't systematically test on old versions */
                if (bootstrapVersion > 3) {
                    modalHeader.appendChild(closeButtonEl);
                }
                else {
                    modalHeader.insertBefore(closeButtonEl, modalHeader.firstChild);
                }
            }
            else {
                body.insertBefore(closeButtonEl, body.firstChild);
            }
        }
        if (finalOptions.centerVertical !== undefined) {
            // Requires Bootstrap 4.0.0 or higher
            /* istanbul ignore if: we don't systematically test with old versions */
            if (fullBootstrapVersion < "4.0.0") {
                // tslint:disable-next-line:no-console
                console.warn("\"centerVertical\" requires Bootstrap 4.0.0 or higher. You appear to be using " + fullBootstrapVersion + ". Please upgrade to use this option.");
            }
            innerDialog.classList.add("modal-dialog-centered");
        }
        // Bootstrap event listeners; these handle extra setup & teardown required
        // after the underlying modal has performed certain actions.
        $modal.one("hidden.bs.modal", function () {
            $modal.off("escape.close.bp");
            $modal.off("click");
            $modal.remove();
        });
        $modal.one("shown.bs.modal", function () {
            // tslint:disable-next-line:no-non-null-assertion
            $(modal.querySelector(".btn-primary")).trigger("focus");
        });
        // Bootprompt event listeners; used to decouple some
        // behaviours from their respective triggers
        if (backdrop !== "static") {
            // A boolean true/false according to the Bootstrap docs
            // should show a dialog the user can dismiss by clicking on
            // the background.
            // We always only ever pass static/false to the actual
            // $.modal function because with "true" we can't trap
            // this event (the .modal-backdrop swallows it)
            // However, we still want to sort of respect true
            // and invoke the escape mechanism instead
            $modal.on("click.dismiss.bs.modal", function (e) {
                // The target varies in 3.3.x releases since the modal backdrop moved
                // *inside* the outer dialog rather than *alongside* it
                var backdrops = modal.getElementsByClassName("modal-backdrop");
                var target = backdrops.length !== 0 ?
                    /* istanbul ignore next: we don't systematically test with 3.3.x */
                    backdrops[0] :
                    e.currentTarget;
                if (e.target !== target) {
                    return;
                }
                $modal.trigger("escape.close.bp");
            });
        }
        $modal.on("escape.close.bp", function (e) {
            // the if statement looks redundant but it isn't; without it
            // if we *didn't* have an onEscape handler then processCallback
            // would automatically dismiss the dialog
            if (callbacks.onEscape === true ||
                typeof callbacks.onEscape === "function") {
                processCallback(e, $modal, callbacks.onEscape);
            }
        });
        $modal.on("click", ".modal-footer button", function (e) {
            var callbackKey = $(this).data("bp-handler");
            processCallback(e, $modal, callbacks[callbackKey]);
        });
        $modal.on("click", ".bootprompt-close-button", function (e) {
            // onEscape might be falsy but that's fine; the fact is
            // if the user has managed to click the close button we
            // have to close the dialog, callback or not
            processCallback(e, $modal, callbacks.onClose);
        });
        $modal.on("keyup", function (e) {
            if (e.which === 27) {
                $modal.trigger("escape.close.bp");
            }
        });
        // The interface defined for $ messes up type inferrence so we have to assert
        // the type here.
        $(finalOptions.container).append($modal);
        $modal.modal({
            backdrop: (backdrop === true || backdrop === "static") ? "static" : false,
            keyboard: false,
            show: false,
        });
        if (finalOptions.show === true) {
            $modal.modal("show");
        }
        return $modal;
    }
    function _alert(options, callback) {
        var finalOptions = mergeDialogOptions("alert", ["ok"], options, callback);
        var finalCallback = finalOptions.callback;
        // tslint:disable-next-line:no-suspicious-comment
        // @TODO: can this move inside exports.dialog when we're iterating over each
        // button and checking its button.callback value instead?
        if (finalCallback !== undefined && typeof finalCallback !== "function") {
            throw new Error("alert requires callback property to be a function when \
provided");
        }
        var customCallback = function () {
            return typeof finalCallback === "function" ?
                finalCallback.call(this) : true;
        };
        finalOptions.buttons.ok.callback = customCallback;
        setupEscapeAndCloseCallbacks(finalOptions, customCallback);
        return dialog(finalOptions);
    }
    function alert(messageOrOptions, callback) {
        return _alert(typeof messageOrOptions === "string" ?
            { message: messageOrOptions } :
            messageOrOptions, callback);
    }
    /**
     * Specialized function that provides a dialog similar to the one provided by
     * the DOM ``alert()`` function.
     *
     * **NOTE**: This function is non-blocking, so any code that must happen after
     * the dialog is dismissed should await the promise returned by this function.
     *
     * @param messageOrOptions The message to display, or an object specifying the
     * options for the dialog.
     *
     * @returns A promise that resolves once the dialog has been dismissed.
     */
    function alert$(messageOrOptions) {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                return [2 /*return*/, new Promise(function (resolve) {
                        _alert(typeof messageOrOptions === "string" ?
                            { message: messageOrOptions } : messageOrOptions, undefined)
                            .one("hidden.bs.modal", function () {
                            resolve();
                        });
                    })];
            });
        });
    }
    function _confirm(options, callback) {
        var finalOptions = mergeDialogOptions("confirm", ["cancel", "confirm"], options, callback);
        var finalCallback = finalOptions.callback, buttons = finalOptions.buttons;
        // confirm specific validation; they don't make sense without a callback so
        // make sure it's present
        if (typeof finalCallback !== "function") {
            throw new Error("confirm requires a callback");
        }
        var cancelCallback = function () {
            return finalCallback.call(this, false);
        };
        buttons.cancel.callback = cancelCallback;
        setupEscapeAndCloseCallbacks(finalOptions, cancelCallback);
        buttons.confirm.callback =
            function () {
                return finalCallback.call(this, true);
            };
        return dialog(finalOptions);
    }
    function confirm(messageOrOptions, callback) {
        return _confirm(typeof messageOrOptions === "string" ?
            { message: messageOrOptions } :
            messageOrOptions, callback);
    }
    /**
     * Specialized function that provides a dialog similar to the one provided by
     * the DOM ``confirm()`` function.
     *
     * **NOTE**: This function is non-blocking, so any code that must happen after
     * the dialog is dismissed should await the promise returned by this function.
     *
     * @param messageOrOptions The message to display, or an object specifying the
     * options for the dialog.
     *
     * @returns A promise that resolves once the dialog has been dismissed.
     */
    function confirm$(messageOrOptions) {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                return [2 /*return*/, new Promise(function (resolve) {
                        var options = typeof messageOrOptions === "string" ?
                            { message: messageOrOptions } : messageOrOptions;
                        var callback = options.callback;
                        var result = null;
                        _confirm(options, function (value) {
                            result = value;
                            if (callback !== undefined) {
                                return callback.call(this, result);
                            }
                        }).one("hidden.bs.modal", function () {
                            resolve(result);
                        });
                    })];
            });
        });
    }
    function setupTextualInput(input, options) {
        var value = options.value, placeholder = options.placeholder, pattern = options.pattern, maxlength = options.maxlength, required = options.required;
        // tslint:disable-next-line:no-non-null-assertion
        input.val(value);
        if (placeholder !== undefined) {
            input.attr("placeholder", placeholder);
        }
        if (pattern !== undefined) {
            input.attr("pattern", pattern);
        }
        if (maxlength !== undefined) {
            input.attr("maxlength", maxlength);
        }
        if (required === true) {
            input.prop({ required: true });
        }
    }
    function setupNumberLikeInput(input, options) {
        var value = options.value, placeholder = options.placeholder, pattern = options.pattern, required = options.required, inputType = options.inputType;
        if (value !== undefined) {
            input.val(String(value));
        }
        if (placeholder !== undefined) {
            input.attr("placeholder", placeholder);
        }
        if (pattern !== undefined) {
            input.attr("pattern", pattern);
        }
        if (required === true) {
            input.prop({ required: true });
        }
        // These input types have extra attributes which affect their input
        // validation.  Warning: For most browsers, date inputs are buggy in their
        // implementation of 'step', so this attribute will have no
        // effect. Therefore, we don't set the attribute for date inputs.  @see
        // tslint:disable-next-line:max-line-length
        // https://developer.mozilla.org/en-US/docs/Web/HTML/Element/input/date#Setting_maximum_and_minimum_dates
        if (inputType !== "date") {
            var step = options.step;
            if (step !== undefined) {
                var stepNumber = Number(step);
                if (step === "any" || (!isNaN(stepNumber) && stepNumber > 0)) {
                    input.attr("step", step);
                }
                else {
                    throw new Error("\"step\" must be a valid positive number or the value \"any\". See https://developer.mozilla.org/en-US/docs/Web/HTML/Element/input#attr-step for more information.");
                }
            }
        }
        validateMinOrMaxValue(input, "min", options);
        validateMinOrMaxValue(input, "max", options);
    }
    function validateInputOptions(inputOptions) {
        var e_4, _a;
        try {
            for (var inputOptions_1 = __values(inputOptions), inputOptions_1_1 = inputOptions_1.next(); !inputOptions_1_1.done; inputOptions_1_1 = inputOptions_1.next()) {
                var _b = inputOptions_1_1.value, value = _b.value, text = _b.text;
                if (value === undefined || text === undefined) {
                    throw new Error("each option needs a \"value\" and a \"text\" property");
                }
                if (typeof value === "number") {
                    throw new Error("bootprompt does not allow numbers for \"value\" in inputOptions");
                }
            }
        }
        catch (e_4_1) { e_4 = { error: e_4_1 }; }
        finally {
            try {
                if (inputOptions_1_1 && !inputOptions_1_1.done && (_a = inputOptions_1.return)) _a.call(inputOptions_1);
            }
            finally { if (e_4) throw e_4.error; }
        }
    }
    function setupSelectInput(input, options) {
        var e_5, _a;
        var inputOptions = options.inputOptions !== undefined ?
            options.inputOptions : [];
        if (!Array.isArray(inputOptions)) {
            throw new Error("Please pass an array of input options");
        }
        if (inputOptions.length === 0) {
            throw new Error("prompt with select requires at least one option \
value");
        }
        var required = options.required, multiple = options.multiple;
        if (required === true) {
            input.prop({ required: true });
        }
        if (multiple === true) {
            input.prop({ multiple: true });
        }
        validateInputOptions(inputOptions);
        var firstValue;
        var groups = Object.create(null);
        try {
            for (var inputOptions_2 = __values(inputOptions), inputOptions_2_1 = inputOptions_2.next(); !inputOptions_2_1.done; inputOptions_2_1 = inputOptions_2.next()) {
                var _b = inputOptions_2_1.value, value = _b.value, text = _b.text, group = _b.group;
                // assume the element to attach to is the input...
                var elem = input[0];
                // ... but override that element if this option sits in a group
                if (group !== undefined && group !== "") {
                    var groupEl = groups[group];
                    if (groupEl === undefined) {
                        groups[group] = groupEl = document.createElement("optgroup");
                        groupEl.setAttribute("label", group);
                    }
                    elem = groupEl;
                }
                var o = $(templates.option);
                o.attr("value", value).text(text);
                elem.appendChild(o[0]);
                if (firstValue === undefined) {
                    firstValue = value;
                }
            }
        }
        catch (e_5_1) { e_5 = { error: e_5_1 }; }
        finally {
            try {
                if (inputOptions_2_1 && !inputOptions_2_1.done && (_a = inputOptions_2.return)) _a.call(inputOptions_2);
            }
            finally { if (e_5) throw e_5.error; }
        }
        // Conditions are such that an undefined firstValue here is an internal error.
        /* istanbul ignore if: we cannot cause this intentionally */
        if (firstValue === undefined) {
            throw new Error("firstValue cannot be undefined at this point");
        }
        // tslint:disable-next-line:forin
        for (var groupName in groups) {
            input.append(groups[groupName]);
        }
        input.val(options.value !== undefined ? options.value : firstValue);
    }
    function setupCheckbox(input, options, inputTemplate) {
        var e_6, _a;
        var checkboxValues = Array.isArray(options.value) ? options.value : [options.value];
        var inputOptions = options.inputOptions !== undefined ?
            options.inputOptions : [];
        if (inputOptions.length === 0) {
            throw new Error("prompt with checkbox requires options");
        }
        validateInputOptions(inputOptions);
        try {
            for (var inputOptions_3 = __values(inputOptions), inputOptions_3_1 = inputOptions_3.next(); !inputOptions_3_1.done; inputOptions_3_1 = inputOptions_3.next()) {
                var _b = inputOptions_3_1.value, value = _b.value, text = _b.text;
                var checkbox = $(inputTemplate);
                checkbox.find("input").attr("value", value);
                checkbox.find("label").append("\n" + text);
                if (checkboxValues.indexOf(value) !== -1) {
                    checkbox.find("input").prop("checked", true);
                }
                input.append(checkbox);
            }
        }
        catch (e_6_1) { e_6 = { error: e_6_1 }; }
        finally {
            try {
                if (inputOptions_3_1 && !inputOptions_3_1.done && (_a = inputOptions_3.return)) _a.call(inputOptions_3);
            }
            finally { if (e_6) throw e_6.error; }
        }
    }
    function setupRadio(input, options, inputTemplate) {
        var e_7, _a;
        // Make sure that value is not an array (only a single radio can ever be
        // checked)
        var initialValue = options.value;
        if (initialValue !== undefined && Array.isArray(initialValue)) {
            throw new Error("prompt with radio requires a single, non-array value for \"value\".");
        }
        var inputOptions = options.inputOptions !== undefined ?
            options.inputOptions : [];
        if (inputOptions.length === 0) {
            throw new Error("prompt with radio requires options");
        }
        validateInputOptions(inputOptions);
        // Radiobuttons should always have an initial checked input checked in a
        // "group".  If value is undefined or doesn't match an input option,
        // select the first radiobutton
        var checkFirstRadio = true;
        try {
            for (var inputOptions_4 = __values(inputOptions), inputOptions_4_1 = inputOptions_4.next(); !inputOptions_4_1.done; inputOptions_4_1 = inputOptions_4.next()) {
                var _b = inputOptions_4_1.value, value = _b.value, text = _b.text;
                var radio = $(inputTemplate);
                radio.find("input").attr("value", value);
                radio.find("label").append("\n" + text);
                if (initialValue !== undefined && value === initialValue) {
                    radio.find("input").prop("checked", true);
                    checkFirstRadio = false;
                }
                input.append(radio);
            }
        }
        catch (e_7_1) { e_7 = { error: e_7_1 }; }
        finally {
            try {
                if (inputOptions_4_1 && !inputOptions_4_1.done && (_a = inputOptions_4.return)) _a.call(inputOptions_4);
            }
            finally { if (e_7) throw e_7.error; }
        }
        if (checkFirstRadio) {
            input.find("input[type='radio']").first().prop("checked", true);
        }
    }
    // tslint:disable-next-line:max-func-body-length
    function _prompt(options, callback) {
        // prompt defaults are more complex than others in that users can override
        // more defaults
        var finalOptions = mergeDialogOptions("prompt", ["cancel", "confirm"], options, callback);
        if (typeof finalOptions.value === "number") {
            throw new Error("bootprompt does not allow numbers as values");
        }
        // capture the user's show value; we always set this to false before spawning
        // the dialog to give us a chance to attach some handlers to it, but we need
        // to make sure we respect a preference not to show it
        var shouldShow = finalOptions.show === undefined ? true : finalOptions.show;
        // This is required prior to calling the dialog builder below - we need to add
        // an event handler just before the prompt is shown
        finalOptions.show = false;
        // prompt-specific validation
        if (finalOptions.title === undefined || finalOptions.title === "") {
            throw new Error("prompt requires a title");
        }
        var finalCallback = finalOptions.callback, buttons = finalOptions.buttons;
        if (typeof finalCallback !== "function") {
            throw new Error("prompt requires a callback");
        }
        if (finalOptions.inputType === undefined) {
            finalOptions.inputType = "text";
        }
        var inputTemplate = templates.inputs[finalOptions.inputType];
        var input;
        switch (finalOptions.inputType) {
            case "text":
            case "textarea":
            case "email":
            case "password":
                input = $(inputTemplate);
                setupTextualInput(input, finalOptions);
                break;
            case "date":
            case "time":
            case "number":
            case "range":
                input = $(inputTemplate);
                setupNumberLikeInput(input, finalOptions);
                break;
            case "select":
                input = $(inputTemplate);
                setupSelectInput(input, finalOptions);
                break;
            case "checkbox":
                // checkboxes have to nest within a containing element
                input = $("<div class=\"bootprompt-checkbox-list\"></div>");
                setupCheckbox(input, finalOptions, inputTemplate);
                break;
            case "radio":
                // radio buttons have to nest within a containing element
                // tslint:disable-next-line:no-jquery-raw-elements
                input = $("<div class='bootprompt-radiobutton-list'></div>");
                setupRadio(input, finalOptions, inputTemplate);
                break;
            default:
                // The type assertion is needed in TS 3.2.4 which is the latest version
                // that typedoc currently runs. *grumble*...
                // tslint:disable-next-line:no-unnecessary-type-assertion
                var q = finalOptions.inputType;
                throw new Error("Unknown input type: " + q);
        }
        var cancelCallback = function () {
            return finalCallback.call(this, null);
        };
        buttons.cancel.callback = cancelCallback;
        setupEscapeAndCloseCallbacks(finalOptions, cancelCallback);
        // Prompt submitted - extract the prompt value. This requires a bit of work,
        // given the different input types available.
        // tslint:disable-next-line:no-non-null-assertion
        buttons.confirm.callback = function () {
            var value;
            switch (finalOptions.inputType) {
                case "checkbox":
                    value = input.find("input:checked")
                        .map(function () {
                        return $(this).val();
                    }).get();
                    break;
                case "radio":
                    value = input.find("input:checked").val();
                    break;
                default:
                    var rawInput = input[0];
                    if (rawInput.checkValidity !== undefined && !rawInput.checkValidity()) {
                        // prevents button callback from being called
                        return false;
                    }
                    if (finalOptions.inputType === "select" &&
                        finalOptions.multiple === true) {
                        value = input.find("option:selected")
                            .map(function () {
                            return $(this).val();
                        }).get();
                    }
                    else {
                        value = input.val();
                    }
            }
            // TS type inferrence fails here.
            // tslint:disable-next-line:no-any
            return finalCallback.call(this, value);
        };
        var form = $(templates.form);
        form.append(input);
        var message = finalOptions.message;
        if (typeof message === "string" && message.trim() !== "") {
            // Add the form to whatever content the user may have added.
            // tslint:disable-next-line:no-inner-html
            form.prepend($(templates.promptMessage).html(message));
        }
        finalOptions.message = form;
        // Generate the dialog
        var promptDialog = dialog(finalOptions);
        form.on("submit", function (e) {
            e.preventDefault();
            // Fix for SammyJS (or similar JS routing library) hijacking the form post.
            e.stopPropagation();
            // tslint:disable-next-line:no-suspicious-comment
            // @TODO can we actually click *the* button object instead?
            // e.g. buttons.confirm.click() or similar
            promptDialog.find(".bootprompt-accept").trigger("click");
        });
        // clear the existing handler focusing the submit button...
        // ...and replace it with one focusing our input, if possible
        promptDialog.off("shown.bs.modal").on("shown.bs.modal", function () {
            input.focus();
        });
        if (shouldShow === true) {
            promptDialog.modal("show");
        }
        return promptDialog;
    }
    // tslint:disable-next-line:max-func-body-length
    function prompt(messageOrOptions, callback) {
        return _prompt(typeof messageOrOptions === "string" ?
            { title: messageOrOptions } :
            messageOrOptions, callback);
    }
    function prompt$(messageOrOptions) {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                return [2 /*return*/, new Promise(function (resolve) {
                        var options = typeof messageOrOptions === "string" ?
                            // tslint:disable-next-line:no-object-literal-type-assertion
                            { title: messageOrOptions } : messageOrOptions;
                        var callback = options.callback;
                        var result = null;
                        _prompt(options, function (value) {
                            result = value;
                            if (callback !== undefined) {
                                // We assert the type of callback because TS's type inference fails
                                // here.
                                // tslint:disable-next-line:no-any
                                return callback.call(this, result);
                            }
                        }).one("hidden.bs.modal", function () {
                            resolve(result);
                        });
                    })];
            });
        });
    }
    //
    // INTERNAL FUNCTIONS
    //
    function setupEscapeAndCloseCallbacks(options, callback) {
        var onEscape = options.onEscape, onClose = options.onClose;
        options.onEscape = (onEscape === undefined || onEscape === true) ?
            callback :
            function (ev) {
                if (onEscape === false || onEscape.call(this, ev) === false) {
                    return false;
                }
                return callback.call(this, ev);
            };
        options.onClose = onClose === undefined ?
            callback :
            function (ev) {
                if (onClose.call(this, ev) === false) {
                    return false;
                }
                return callback.call(this, ev);
            };
    }
    /**
     * Get localized text from a locale. Defaults to ``en`` locale if no locale
     * provided or a non-registered locale is requested.
     *
     * @param key The field to get from the locale.
     *
     * @param locale The locale name.
     *
     * @returns The field from the locale.
     */
    function getText(key, locale) {
        var labels = definedLocales[locale];
        return labels !== undefined ? labels[key] : definedLocales.en[key];
    }
    /**
     *
     * Make buttons from a series of labels. All this does is normalise the given
     * labels and translate them where possible.
     *
     * @param labels The button labels.
     *
     * @param locale A locale name.
     *
     * @returns The created buttons.
     *
     */
    function makeButtons(labels, locale) {
        var e_8, _a;
        var buttons = Object.create(null);
        try {
            for (var labels_1 = __values(labels), labels_1_1 = labels_1.next(); !labels_1_1.done; labels_1_1 = labels_1.next()) {
                var label = labels_1_1.value;
                buttons[label.toLowerCase()] = {
                    label: getText(label.toUpperCase(), locale),
                };
            }
        }
        catch (e_8_1) { e_8 = { error: e_8_1 }; }
        finally {
            try {
                if (labels_1_1 && !labels_1_1.done && (_a = labels_1.return)) _a.call(labels_1);
            }
            finally { if (e_8) throw e_8.error; }
        }
        return buttons;
    }
    /**
     * Produce a DialogOptions object from the options, or arguments passed to the
     * specialized functions (alert, confirm, prompt).
     *
     * @param kind The kind of specialized function that was called.
     *
     * @param labels The button labels that the specialized function uses.
     *
     * @param options: The first argument of the specialized functions is either an
     * options object, or a string. The value of that first argument must be passed
     * here.
     *
     * @returns Options to pass to [[dialog]].
     */
    function mergeDialogOptions(kind, labels, options, callback) {
        // An earlier implementation was building a hash from ``buttons``. However,
        // the ``buttons`` array is very small. Profiling in other projects have shown
        // that for very small arrays, there's no benefit to creating a table for
        // lookup.
        //
        // An earlier implementation was also performing the check on the merged
        // options (the return value of this function) but that was pointless as it is
        // not possible to add invalid buttons with makeButtons.
        //
        for (var key in options.buttons) {
            // tslint:disable-next-line:no-any
            if (labels.indexOf(key) === -1) {
                throw new Error("button key \"" + key + "\" is not allowed (options are " + labels.join(" ") + ")");
            }
        }
        var locale = options.locale, swapButtonOrder = options.swapButtonOrder;
        return $.extend(true, // deep merge
        Object.create(null), {
            className: "bootprompt-" + kind,
            buttons: makeButtons(swapButtonOrder === true ? labels.slice().reverse() :
                labels, locale !== undefined ? locale : currentLocale),
        }, options, { callback: callback });
    }
    //  Filter and tidy up any user supplied parameters to this dialog.
    //  Also looks for any shorthands used and ensures that the options
    //  which are returned are all normalized properly
    function sanitize(options) {
        if (typeof options !== "object") {
            throw new Error("Please supply an object of options");
        }
        if (options.message === undefined) {
            throw new Error("Please specify a message");
        }
        var finalOptions = __assign({ locale: currentLocale, backdrop: "static", animate: animate, closeButton: true, show: true, container: document.body }, options);
        // no buttons is still a valid dialog but it's cleaner to always have
        // a buttons object to iterate over, even if it's empty
        var buttons = finalOptions.buttons;
        if (buttons === undefined) {
            buttons = finalOptions.buttons = Object.create(null);
        }
        var total = Object.keys(buttons).length;
        var index = 0;
        // tslint:disable-next-line:forin
        for (var key in buttons) {
            var button = buttons[key];
            if (typeof button === "function") {
                // short form, assume value is our callback. Since button
                // isn't an object it isn't a reference either so re-assign it
                button = buttons[key] = {
                    callback: button,
                };
            }
            // before any further checks make sure by now button is the correct type
            if (typeof button !== "object") {
                throw new Error("button with key \"" + key + "\" must be an object");
            }
            if (button.label === undefined) {
                // the lack of an explicit label means we'll assume the key is good enough
                button.label = key;
            }
            if (button.className === undefined) {
                var isPrimary = index === (finalOptions.swapButtonOrder === true ? 0 : total - 1);
                // always add a primary to the main option in a one or two-button dialog
                button.className = (total <= 2 && isPrimary) ?
                    "btn-primary" :
                    "btn-secondary btn-default";
            }
            index++;
        }
        // TS cannot infer that we have SanitizedDialogOptions at this point.
        return finalOptions;
    }
    function throwMaxMinError(name) {
        throw new Error("\"max\" must be greater than \"min\". See https://developer.mozilla.org/en-US/docs/Web/HTML/Element/input#attr-" + name + " for more information.");
    }
    //  Handle the invoked dialog callback
    function processCallback(e, $forDialog, callback) {
        e.stopPropagation();
        e.preventDefault();
        // By default we assume a callback will get rid of the dialog, although it is
        // given the opportunity to override this so, if the callback can be invoked
        // and it *explicitly returns false* then we keep the dialog active...
        // otherwise we'll bin it
        if (!(typeof callback === "function" &&
            callback.call($forDialog, e) === false)) {
            $forDialog.modal("hide");
        }
    }
    // Helper function, since the logic for validating min and max attributes is
    // almost identical
    function validateMinOrMaxValue(input, name, options) {
        var value = options[name];
        if (value === undefined) {
            return;
        }
        var compareValue = options[name === "min" ? "max" : "min"];
        input.attr(name, value);
        var min = options.min, max = options.max;
        // Type inference fails to realize the real type of value...
        switch (options.inputType) {
            case "date":
                /* istanbul ignore if: we don't test the positive case */
                if (!/(\d{4})-(\d{2})-(\d{2})/.test(value)) {
                    // tslint:disable-next-line:no-console
                    console.warn("Browsers which natively support the \"date\" input type expect date values to be of the form \"YYYY-MM-DD\" (see ISO-8601 https://www.iso.org/iso-8601-date-and-time-format.html). Bootprompt does not enforce this rule, but your " + name + " value may not be enforced by this browser.");
                }
                break;
            case "time":
                if (!/([01][0-9]|2[0-3]):[0-5][0-9]?:[0-5][0-9]/.test(value)) {
                    throw new Error("\"" + name + "\" is not a valid time. See https://www.w3.org/TR/2012/WD-html-markup-20120315/datatypes.html#form.data.time for more information.");
                }
                // tslint:disable-next-line:no-non-null-assertion
                if (!(compareValue === undefined || max > min)) {
                    return throwMaxMinError(name);
                }
                break;
            default:
                // Yes we force the string into isNaN. It works.
                if (isNaN(value)) {
                    throw new Error("\"" + name + "\" must be a valid number. See https://developer.mozilla.org/en-US/docs/Web/HTML/Element/input#attr-" + name + " for more information.");
                }
                var minNumber = Number(min);
                var maxNumber = Number(max);
                // tslint:disable-next-line:no-non-null-assertion
                if (!(compareValue === undefined || maxNumber > minNumber) &&
                    // Yes we force the string into isNaN. It works.
                    !isNaN(compareValue)) {
                    return throwMaxMinError(name);
                }
        }
    }
    //  Register the default locale
    addLocale("en", {
        OK: "OK",
        CANCEL: "Cancel",
        CONFIRM: "OK",
    });

    exports.VERSION = VERSION;
    exports.addLocale = addLocale;
    exports.alert = alert;
    exports.alert$ = alert$;
    exports.confirm = confirm;
    exports.confirm$ = confirm$;
    exports.dialog = dialog;
    exports.hideAll = hideAll;
    exports.locales = locales;
    exports.prompt = prompt;
    exports.prompt$ = prompt$;
    exports.removeLocale = removeLocale;
    exports.setAnimate = setAnimate;
    exports.setLocale = setLocale;

    Object.defineProperty(exports, '__esModule', { value: true });

}));
//# sourceMappingURL=bootprompt.js.map