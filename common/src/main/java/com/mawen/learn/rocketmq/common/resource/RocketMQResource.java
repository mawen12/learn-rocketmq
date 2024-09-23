package com.mawen.learn.rocketmq.common.resource;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/9/22
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface RocketMQResource {

	ResourceType value();

	String splitter() default "";
}
