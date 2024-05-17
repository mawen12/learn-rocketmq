package com.mawen.learn.rocketmq.spring;

import java.util.List;

import com.mawen.learn.rocketmq.spring.config.RocketMQDemoProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.rocketmq.spring.core.RocketMQTemplate;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/5/16
 */
@Slf4j
@SpringBootApplication
public class RocketMQApplication implements CommandLineRunner {

	@Autowired
	private RocketMQDemoProperties properties;

	@Autowired
	private RocketMQTemplate rocketMQTemplate;

	public static void main(String[] args) {
		SpringApplication.run(RocketMQApplication.class, args);
	}

	@Override
	public void run(String... args) {
		List<String> messages = rocketMQTemplate.receive(String.class);
		log.info("receive from rocketMQTemplate, messages = {}", messages);
	}
}
