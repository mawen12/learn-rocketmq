package com.mawen.learn.rocketmq.client.hook;

import com.mawen.learn.rocketmq.client.impl.CommunicationMode;
import com.mawen.learn.rocketmq.client.producer.SendResult;
import com.mawen.learn.rocketmq.common.message.Message;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/24
 */
@Getter
@Setter
@ToString
public class CheckForbiddenContext {

	private String nameSrvAddr;

	private String group;

	private Message message;

	private MessageQueue mq;

	private String brokerAddr;

	private CommunicationMode communicationMode;

	private SendResult sendResult;

	private Exception exception;

	private Object arg;

	private boolean unitMode = false;
}
