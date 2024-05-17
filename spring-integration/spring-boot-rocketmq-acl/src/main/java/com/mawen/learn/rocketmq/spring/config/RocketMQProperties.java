package com.mawen.learn.rocketmq.spring.config;

import lombok.Getter;
import lombok.Setter;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/5/17
 */
@Getter
@Setter
@Component
@ConfigurationProperties("demo.rocketmq")
public class RocketMQProperties {

	private String transTopic;

	private String topic;
}
