package com.mawen.learn.rocketmq.remoting.protocol.header;

import com.mawen.learn.rocketmq.common.action.Action;
import com.mawen.learn.rocketmq.common.action.RocketMQAction;
import com.mawen.learn.rocketmq.common.resource.ResourceType;
import com.mawen.learn.rocketmq.common.resource.RocketMQResource;
import com.mawen.learn.rocketmq.remoting.annotation.CFNotNull;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;
import com.mawen.learn.rocketmq.remoting.protocol.RequestCode;
import com.mawen.learn.rocketmq.remoting.rpc.RpcRequestHeader;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
@RocketMQAction(value = RequestCode.CHECK_TRANSACTION_STATE, action = Action.SUB)
public class CheckTransactionStateRequestHeader extends RpcRequestHeader {

	@RocketMQResource(ResourceType.TOPIC)
	private String topic;

	@CFNotNull
	private Long tranStateTableOffset;

	@CFNotNull
	private Long commitLogOffset;

	private String msgId;

	private String transactionId;

	private String offsetMsgId;

	@Override
	public void checkFields() throws RemotingCommandException {

	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public Long getTranStateTableOffset() {
		return tranStateTableOffset;
	}

	public void setTranStateTableOffset(Long tranStateTableOffset) {
		this.tranStateTableOffset = tranStateTableOffset;
	}

	public Long getCommitLogOffset() {
		return commitLogOffset;
	}

	public void setCommitLogOffset(Long commitLogOffset) {
		this.commitLogOffset = commitLogOffset;
	}

	public String getMsgId() {
		return msgId;
	}

	public void setMsgId(String msgId) {
		this.msgId = msgId;
	}

	public String getTransactionId() {
		return transactionId;
	}

	public void setTransactionId(String transactionId) {
		this.transactionId = transactionId;
	}

	public String getOffsetMsgId() {
		return offsetMsgId;
	}

	public void setOffsetMsgId(String offsetMsgId) {
		this.offsetMsgId = offsetMsgId;
	}

	@Override
	public String toString() {
		return "CheckTransactionStateRequestHeader{" +
				"topic='" + topic + '\'' +
				", tranStateTableOffset=" + tranStateTableOffset +
				", commitLogOffset=" + commitLogOffset +
				", msgId='" + msgId + '\'' +
				", transactionId='" + transactionId + '\'' +
				", offsetMsgId='" + offsetMsgId + '\'' +
				"} " + super.toString();
	}
}
