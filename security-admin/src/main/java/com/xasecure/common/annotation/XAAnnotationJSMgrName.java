/**
 *
 */
package com.xasecure.common.annotation;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * 
 *
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface XAAnnotationJSMgrName {
    public String value();
}
