package com.mawen.learn.rocketmq.client.hook;

import java.util.Map;

import com.mawen.learn.rocketmq.client.impl.CommunicationMode;
import com.mawen.learn.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import com.mawen.learn.rocketmq.client.producer.SendResult;
import com.mawen.learn.rocketmq.common.message.Message;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.common.message.MessageType;
import lombok.Getter;
import lombok.Setter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/18
 */
@Getter
@Setter
public class SendMessageContext {

	private String producerGroup;
	private Message message;
	private MessageQueue mq;
	private String brokerAddr;
	private String bornHost;
	private CommunicationMode communicationMode;
	private SendResult sendResult;
	private Exception exception;
	private Object mqTraceContext;
	private Map<String, String> props;
	private DefaultMQProducerImpl producer;
	private MessageType msgType = MessageType.Normal_Msg;
	private String namespace;
}
