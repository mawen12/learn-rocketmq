package com.mawen.learn.rocketmq.client.hook;

import java.util.List;
import java.util.Map;

import com.mawen.learn.rocketmq.client.AccessChannel;
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
public class ConsumeMessageContext {

	private String consumerGroup;
	private List<MessageExt> msgList;
	private MessageQueue mq;
	private boolean success;
	private String status;
	private Object mqTraceContext;
	private Map<String, String> props;
	private String namespace;
	private AccessChannel accessChannel;
}
