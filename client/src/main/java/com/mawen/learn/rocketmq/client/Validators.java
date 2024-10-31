package com.mawen.learn.rocketmq.client;

import java.io.File;
import java.util.Properties;

import com.mawen.learn.rocketmq.client.exception.MQClientException;
import com.mawen.learn.rocketmq.client.producer.DefaultMQProducer;
import com.mawen.learn.rocketmq.common.TopicConfig;
import com.mawen.learn.rocketmq.common.UtilAll;
import com.mawen.learn.rocketmq.common.constant.PermName;
import com.mawen.learn.rocketmq.common.message.Message;
import com.mawen.learn.rocketmq.common.message.MessageConst;
import com.mawen.learn.rocketmq.common.topic.TopicValidator;
import com.mawen.learn.rocketmq.remoting.protocol.ResponseCode;
import org.apache.commons.lang3.StringUtils;

import static com.mawen.learn.rocketmq.common.topic.TopicValidator.*;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/22
 */
public class Validators {

	public static final int CHARACTER_MAX_LENGTH = 256;
	public static final int TOPIC_MAX_LENGTH = 127;

	public static void checkGroup(String group) throws MQClientException {
		if (UtilAll.isBlank(group)) {
			throw new MQClientException("the specified group is blank", null);
		}

		if (group.length() > CHARACTER_MAX_LENGTH) {
			throw new MQClientException("the specified group is longer that group max length 255", null);
		}

		if (isTopicOrGroupIllegal(group)) {
			throw new MQClientException(String.format("the specified group[%s] contains illegal characters, allowing only %s",
					group, "^[%|a-zA-Z0-9_-]+$"), null);
		}
	}

	public static void checkMessage(Message msg, DefaultMQProducer defaultMQProducer) throws MQClientException {
		if (msg == null) {
			throw new MQClientException(ResponseCode.MESSAGE_ILLEGAL, "the message is null");
		}

		Validators.checkTopic(msg.getTopic());
		Validators.isNotAllowedSendTopic(msg.getTopic());

		if (msg.getBody() == null) {
			throw new MQClientException(ResponseCode.MESSAGE_ILLEGAL, "the message body is null");
		}

		if (msg.getBody().length == 0) {
			throw new MQClientException(ResponseCode.MESSAGE_ILLEGAL, "the message body length is zero");
		}

		if (msg.getBody().length > defaultMQProducer.getMaxMessageSize()) {
			throw new MQClientException(ResponseCode.MESSAGE_ILLEGAL, "the message body size over max value, MAX: " + defaultMQProducer.getMaxMessageSize());
		}

		String lmqPath = msg.getUserProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH);
		if (StringUtils.contains(lmqPath, File.separator)) {
			throw new MQClientException(ResponseCode.MESSAGE_ILLEGAL, "INNER_MULTI_DISPATCH " + lmqPath + " can not contains " + File.separator + " character");
		}
	}

	public static void checkTopic(String topic) throws MQClientException {
		if (UtilAll.isBlank(topic)) {
			throw new MQClientException("The specified topic is blank", null);
		}

		if (topic.length() > TOPIC_MAX_LENGTH) {
			throw new MQClientException(String.format("The specified topic is longer that topic max length %d", TOPIC_MAX_LENGTH), null);
		}

		if (isTopicOrGroupIllegal(topic)) {
			throw new MQClientException(String.format("The specified topic[%s] contains illegal characters, allowing only %s",
					topic, "^[%|a-zA-Z0-9_-]+$"), null);
		}
	}

	public static void isSystemTopic(String topic) throws MQClientException {
		if (TopicValidator.isSystemTopic(topic)) {
			throw new MQClientException(String.format("The topic[%s] is conflict with system topic.", topic), null);
		}
	}

	public static void isNotAllowedSendTopic(String topic) throws MQClientException {
		if (TopicValidator.isNotAllowedSendTopic(topic)) {
			throw new MQClientException(String.format("Sending message to topic[%s] is forbidden.", topic), null);
		}
	}

	public static void checkTopicConfig(final TopicConfig topicConfig) throws MQClientException {
		if (!PermName.isValid(topicConfig.getPerm())) {
			throw new MQClientException(ResponseCode.NO_PERMISSION, String.format("topicPermission value: %s is valid.", topicConfig.getPerm()));
		}
	}

	public static void checkBrokerConfig(final Properties brokerConfig) throws MQClientException {
		if (brokerConfig.containsKey("brokerPermission") && !PermName.isValid(brokerConfig.getProperty("brokerPermission"))) {
			throw new MQClientException(ResponseCode.NO_PERMISSION, String.format("brokerPermission value: %s is invalid.", brokerConfig.getProperty("brokerPermission")));
		}
	}
}
