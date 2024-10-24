package com.mawen.learn.rocketmq.client.producer;

import com.alibaba.fastjson.JSON;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/16
 */
@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class SendResult {

	private SendStatus sendStatus;

	private String msgId;

	private MessageQueue messageQueue;

	private long queueOffset;

	private String transactionId;

	private String offsetMsgId;

	private String regionId;

	private boolean traceOn = true;

	private byte[] rawRespBody;

	public SendResult(SendStatus sendStatus, String msgId, String offsetMsgId, MessageQueue messageQueue, long queueOffset) {
		this.sendStatus = sendStatus;
		this.msgId = msgId;
		this.messageQueue = messageQueue;
		this.queueOffset = queueOffset;
		this.offsetMsgId = offsetMsgId;
	}

	public SendResult(final SendStatus sendStatus, final String msgId, final MessageQueue mq, final long queueOffset, final String transactionId, final String offsetMsgId, final String regionId) {
		this.sendStatus = sendStatus;
		this.msgId = msgId;
		this.messageQueue = mq;
		this.queueOffset = queueOffset;
		this.transactionId = transactionId;
		this.offsetMsgId = offsetMsgId;
		this.regionId = regionId;
	}

	public static String encoderSendResultToJson(final Object obj) {
		return JSON.toJSONString(obj);
	}

	public static SendResult decoderJsonToSendResult(String json) {
		return JSON.parseObject(json, SendResult.class);
	}
}
