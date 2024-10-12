package com.mawen.learn.rocketmq.remoting.protocol.body;

import java.util.List;

import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
public class BatchAckMessageRequestBody extends RemotingSerializable {

	private String brokerName;

	private List<BatchAck> acks;

	public String getBrokerName() {
		return brokerName;
	}

	public void setBrokerName(String brokerName) {
		this.brokerName = brokerName;
	}

	public List<BatchAck> getAcks() {
		return acks;
	}

	public void setAcks(List<BatchAck> acks) {
		this.acks = acks;
	}
}
