package com.mawen.learn.rocketmq.spring.config;

import lombok.Getter;
import lombok.Setter;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/5/16
 */
@Getter
@Setter
@Component
@ConfigurationProperties(prefix = "demo.rocketmq")
public class RocketMQDemoProperties {

	private String tag;

	private String transTopic;

	private String topic;

	private String userTopic;

	private String orderTopic;

	private String msgExtTopic;

	private String stringRequestTopic;

	private String bytesRequestTopic;

	private String objectRequestTopic;

	private String genericRequestTopic;
}
