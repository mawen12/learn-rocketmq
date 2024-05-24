package com.mawen.learn.rocketmq5.spring.config;

import lombok.Data;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/5/24
 */
@Data
@Component
@ConfigurationProperties(prefix = "demo.rocketmq")
public class DemoProperties {

	private String fifoTopic;

	private String delayTopic;

	private String transTopic;

	private String normalTopic;

	private String messageGroup;
}
