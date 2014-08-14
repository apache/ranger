/**
 *
 */
package com.xasecure.common.annotation;

import java.lang.annotation.*;

/**
 * Annotating the REST APIs
 *
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface XAAnnotationRestAPI {
    public String api_name() default "";
    public boolean updates_generic_objects() default false;
    public String updates_classes() default "";
}
