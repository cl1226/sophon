package com.scistor.compute.annotation;

import java.lang.annotation.*;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface HandlerMapping {

    String value();

    String httpMethod() default "get";
}
