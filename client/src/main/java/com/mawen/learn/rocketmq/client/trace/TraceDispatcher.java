package com.mawen.learn.rocketmq.client.trace;

import java.io.IOException;

import com.mawen.learn.rocketmq.client.AccessChannel;
import com.mawen.learn.rocketmq.client.exception.MQClientException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/17
 */
public interface TraceDispatcher {

	void start(String namesrvAddr, AccessChannel accessChannel) throws MQClientException;

	boolean append(Object obj);

	void flush() throws IOException;

	void shutdown();

	enum Type {
		PRODUCE,

		CONSUME,
	}
}
