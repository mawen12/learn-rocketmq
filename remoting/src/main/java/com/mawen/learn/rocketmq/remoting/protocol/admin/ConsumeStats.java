package com.mawen.learn.rocketmq.remoting.protocol.admin;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
public class ConsumeStats extends RemotingSerializable {

	private ConcurrentMap<MessageQueue, OffsetWrapper> offsetTable = new ConcurrentHashMap<>();

	private double consumeTps = 0;

	public long computeTotalDiff() {
		return this.offsetTable.values().stream().mapToLong(v -> v.getBrokerOffset() - v.getConsumerOffset()).sum();
	}

	public long computeInflightTotalDiff() {
		return this.offsetTable.values().stream().mapToLong(v -> v.getPullOffset() - v.getConsumerOffset()).sum();
	}

	public ConcurrentMap<MessageQueue, OffsetWrapper> getOffsetTable() {
		return offsetTable;
	}

	public void setOffsetTable(ConcurrentMap<MessageQueue, OffsetWrapper> offsetTable) {
		this.offsetTable = offsetTable;
	}

	public double getConsumeTps() {
		return consumeTps;
	}

	public void setConsumeTps(double consumeTps) {
		this.consumeTps = consumeTps;
	}
}
