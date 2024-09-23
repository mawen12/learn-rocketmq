package com.mawen.learn.rocketmq.common.action;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import com.mawen.learn.rocketmq.common.resource.ResourceType;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/9/22
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface RocketMQAction {

	int value();

	ResourceType resource() default ResourceType.UNKNOWN;

	Action[] action();
}
