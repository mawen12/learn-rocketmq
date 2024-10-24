package com.mawen.learn.rocketmq.client.utils;

import com.mawen.learn.rocketmq.client.common.ClientErrorCode;
import com.mawen.learn.rocketmq.client.exception.MQClientException;
import com.mawen.learn.rocketmq.common.MixAll;
import com.mawen.learn.rocketmq.common.message.Message;
import com.mawen.learn.rocketmq.common.message.MessageAccessor;
import com.mawen.learn.rocketmq.common.message.MessageConst;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/24
 */
public class MessageUtil {

	public static Message createReplyMessage(final Message request, final byte[] body) throws MQClientException {
		if (request != null) {
			Message reply = new Message();
			String cluster = request.getProperty(MessageConst.PROPERTY_CLUSTER);
			String replyTo = request.getProperty(MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT);
			String correlationId = request.getProperty(MessageConst.PROPERTY_CORRELATION_ID);
			String ttl = request.getProperty(MessageConst.PROPERTY_MESSAGE_TTL);

			reply.setBody(body);

			if (cluster != null) {
				String replyTopic = MixAll.getReplyTopic(cluster);
				reply.setTopic(replyTopic);
				MessageAccessor.putProperty(reply, MessageConst.PROPERTY_MESSAGE_TYPE, MixAll.REPLY_MESSAGE_FLAG);
				MessageAccessor.putProperty(reply, MessageConst.PROPERTY_CORRELATION_ID, correlationId);
				MessageAccessor.putProperty(reply, MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT, replyTo);
				MessageAccessor.putProperty(reply, MessageConst.PROPERTY_MESSAGE_TTL, ttl);

				return reply;
			}
			else {
				throw new MQClientException(ClientErrorCode.CREATE_REPLY_MESSAGE_EXCEPTION, "create reply message fail, requestMessage error, property[" + MessageConst.PROPERTY_CLUSTER + "] is null,");
			}
		}
		throw new MQClientException(ClientErrorCode.CREATE_REPLY_MESSAGE_EXCEPTION, "create reply message fail, requestMessage cannot be null.");
	}

	public static String getReplyToClient(final Message msg) {
		return msg.getProperty(MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT);
	}
}
