package com.mawen.learn.rocketmq.client.hook;

import java.util.List;

import com.mawen.learn.rocketmq.common.message.MessageExt;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/23
 */
@Getter
@Setter
@ToString
public class FilterMessageContext {

	private String consumerGroup;
	private List<MessageExt> msgList;
	private MessageQueue mq;
	private Object arg;
	private boolean unitMode;
}
