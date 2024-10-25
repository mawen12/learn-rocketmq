package com.mawen.learn.rocketmq.client.latency;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/25
 */
public interface Resolver {

	String resolve(String name);
}
