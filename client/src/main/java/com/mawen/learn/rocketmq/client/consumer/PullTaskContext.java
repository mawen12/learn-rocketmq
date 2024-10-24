package com.mawen.learn.rocketmq.client.consumer;

import lombok.Getter;
import lombok.Setter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/24
 */
@Getter
@Setter
public class PullTaskContext {

	private int pullNextDelayTimeMillis = 200;

	private MQPullConsumer pullConsumer;
}
