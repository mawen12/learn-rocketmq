package com.mawen.learn.rocketmq4.spring;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.elasticsearch.repository.config.EnableElasticsearchRepositories;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/5/27
 */
@SpringBootApplication
@EnableElasticsearchRepositories("com.mawen.learn.rocketmq4.spring.repository.elasticsearch")
@EnableJpaRepositories("com.mawen.learn.rocketmq4.spring.repository.mysql")
public class BatchProcessApp {

	public static void main(String[] args) {
		SpringApplication.run(BatchProcessApp.class, args);
	}
}
