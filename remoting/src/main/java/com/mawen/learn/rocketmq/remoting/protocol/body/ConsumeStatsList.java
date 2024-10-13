package com.mawen.learn.rocketmq.remoting.protocol.body;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;
import com.mawen.learn.rocketmq.remoting.protocol.admin.ConsumeStats;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/13
 */
public class ConsumeStatsList extends RemotingSerializable {

	private List<Map<String, List<ConsumeStats>>> consumeStatsList = new ArrayList<>();

	private String brokerAddr;

	private long totalDiff;

	private long totalInflightDiff;

	public List<Map<String, List<ConsumeStats>>> getConsumeStatsList() {
		return consumeStatsList;
	}

	public void setConsumeStatsList(List<Map<String, List<ConsumeStats>>> consumeStatsList) {
		this.consumeStatsList = consumeStatsList;
	}

	public String getBrokerAddr() {
		return brokerAddr;
	}

	public void setBrokerAddr(String brokerAddr) {
		this.brokerAddr = brokerAddr;
	}

	public long getTotalDiff() {
		return totalDiff;
	}

	public void setTotalDiff(long totalDiff) {
		this.totalDiff = totalDiff;
	}

	public long getTotalInflightDiff() {
		return totalInflightDiff;
	}

	public void setTotalInflightDiff(long totalInflightDiff) {
		this.totalInflightDiff = totalInflightDiff;
	}
}
