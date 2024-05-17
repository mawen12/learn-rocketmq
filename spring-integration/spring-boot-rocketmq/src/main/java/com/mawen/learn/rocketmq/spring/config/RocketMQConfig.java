package com.mawen.learn.rocketmq.spring.config;

import lombok.Value;
import org.apache.rocketmq.spring.annotation.ExtRocketMQTemplateConfiguration;
import org.apache.rocketmq.spring.core.RocketMQTemplate;

import org.springframework.context.annotation.Configuration;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/5/16
 */
@Configuration
public class RocketMQConfig {



//	@ExtRocketMQTemplateConfiguration(nameServer = "${demo.rocketmq.extNameServer}", tlsEnable = "${demo.rocketmq.ext.useTLS}", instanceName = "pztest33")
	public static class ExtRocketMQTemplate extends RocketMQTemplate {

	}
}
