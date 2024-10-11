package com.mawen.learn.rocketmq.remoting.protocol.header;

import java.util.Map;

import com.mawen.learn.rocketmq.remoting.CommandCustomHeader;
import com.mawen.learn.rocketmq.remoting.annotation.CFNotNull;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;
import com.mawen.learn.rocketmq.remoting.protocol.FastCodesHeader;
import io.netty.buffer.ByteBuf;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/11
 */
public class SendMessageResponseHeader implements CommandCustomHeader, FastCodesHeader {

	@CFNotNull
	private String msgId;

	@CFNotNull
	private Integer queueId;

	@CFNotNull
	private Long queueOffset;

	private String transactionId;

	private String batchUniqId;

	@Override
	public void checkFields() throws RemotingCommandException {

	}

	@Override
	public void encode(ByteBuf out) {
		writeIfNotNull(out, "msgId", msgId);
		writeIfNotNull(out, "queueId", queueId);
		writeIfNotNull(out, "queueOffset", queueOffset);
		writeIfNotNull(out, "transactionId", transactionId);
		writeIfNotNull(out, "batchUniqId", batchUniqId);
	}

	@Override
	public void decode(Map<String, String> fields) throws RemotingCommandException {
		this.msgId = getAndCheckNotNull(fields, "msgId");

		this.queueId = Integer.valueOf(getAndCheckNotNull(fields, "queueId"));

		this.queueOffset = Long.valueOf(getAndCheckNotNull(fields, "queueOffset"));

		String str = fields.get("transactionId");
		if (str != null) {
			this.transactionId = str;
		}

		str = fields.get("batchUniqId");
		if (str != null) {
			this.batchUniqId = str;
		}
	}

	public String getMsgId() {
		return msgId;
	}

	public void setMsgId(String msgId) {
		this.msgId = msgId;
	}

	public Integer getQueueId() {
		return queueId;
	}

	public void setQueueId(Integer queueId) {
		this.queueId = queueId;
	}

	public Long getQueueOffset() {
		return queueOffset;
	}

	public void setQueueOffset(Long queueOffset) {
		this.queueOffset = queueOffset;
	}

	public String getTransactionId() {
		return transactionId;
	}

	public void setTransactionId(String transactionId) {
		this.transactionId = transactionId;
	}

	public String getBatchUniqId() {
		return batchUniqId;
	}

	public void setBatchUniqId(String batchUniqId) {
		this.batchUniqId = batchUniqId;
	}
}
