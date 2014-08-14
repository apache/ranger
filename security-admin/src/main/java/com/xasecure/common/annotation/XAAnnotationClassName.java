/**
 *
 */
package com.xasecure.common.annotation;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;


@Retention(RetentionPolicy.RUNTIME)
public @interface XAAnnotationClassName {
    public Class class_name();
}
