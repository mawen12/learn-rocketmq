package com.mawen.learn.rocketmq.remoting.protocol.header;

import com.mawen.learn.rocketmq.common.action.Action;
import com.mawen.learn.rocketmq.common.action.RocketMQAction;
import com.mawen.learn.rocketmq.common.resource.ResourceType;
import com.mawen.learn.rocketmq.common.resource.RocketMQResource;
import com.mawen.learn.rocketmq.common.sysflag.MessageSysFlag;
import com.mawen.learn.rocketmq.remoting.annotation.CFNotNull;
import com.mawen.learn.rocketmq.remoting.annotation.CFNullable;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;
import com.mawen.learn.rocketmq.remoting.protocol.RequestCode;
import com.mawen.learn.rocketmq.remoting.rpc.RpcRequestHeader;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
@RocketMQAction(value = RequestCode.END_TRANSACTION, action = Action.PUB)
public class EndTransactionRequestHeader extends RpcRequestHeader {

	@RocketMQResource(ResourceType.TOPIC)
	private String topic;

	@CFNotNull
	private String producerGroup;

	@CFNotNull
	private String tranStateTableOffset;

	@CFNotNull
	private Long commitLogOffset;

	@CFNotNull
	private Integer commitOrRollback;

	@CFNullable
	private Boolean fromTransactionCheck = false;

	@CFNotNull
	private String msgId;

	private String transactionId;

	@Override
	public void checkFields() throws RemotingCommandException {
		if (MessageSysFlag.TRANSACTION_NOT_TYPE == this.commitOrRollback) {
			return;
		}

		if (MessageSysFlag.TRANSACTION_COMMIT_TYPE == this.commitOrRollback) {
			return;
		}

		if (MessageSysFlag.TRANSACTION_ROLLBACK_TYPE == this.commitOrRollback) {
			return;
		}

		throw new RemotingCommandException("commitOrRollback field wrong");
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public String getProducerGroup() {
		return producerGroup;
	}

	public void setProducerGroup(String producerGroup) {
		this.producerGroup = producerGroup;
	}

	public String getTranStateTableOffset() {
		return tranStateTableOffset;
	}

	public void setTranStateTableOffset(String tranStateTableOffset) {
		this.tranStateTableOffset = tranStateTableOffset;
	}

	public Long getCommitLogOffset() {
		return commitLogOffset;
	}

	public void setCommitLogOffset(Long commitLogOffset) {
		this.commitLogOffset = commitLogOffset;
	}

	public Integer getCommitOrRollback() {
		return commitOrRollback;
	}

	public void setCommitOrRollback(Integer commitOrRollback) {
		this.commitOrRollback = commitOrRollback;
	}

	public Boolean getFromTransactionCheck() {
		return fromTransactionCheck;
	}

	public void setFromTransactionCheck(Boolean fromTransactionCheck) {
		this.fromTransactionCheck = fromTransactionCheck;
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

	@Override
	public String toString() {
		return "EndTransactionRequestHeader{" +
				"topic='" + topic + '\'' +
				", producerGroup='" + producerGroup + '\'' +
				", tranStateTableOffset='" + tranStateTableOffset + '\'' +
				", commitLogOffset=" + commitLogOffset +
				", commitOrRollback=" + commitOrRollback +
				", fromTransactionCheck=" + fromTransactionCheck +
				", msgId='" + msgId + '\'' +
				", transactionId='" + transactionId + '\'' +
				"} " + super.toString();
	}
}
