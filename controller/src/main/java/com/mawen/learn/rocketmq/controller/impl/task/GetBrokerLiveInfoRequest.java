package com.mawen.learn.rocketmq.controller.impl.task;

import com.mawen.learn.rocketmq.controller.impl.heartbeat.BrokerIdentityInfo;
import com.mawen.learn.rocketmq.remoting.CommandCustomHeader;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;
import lombok.Getter;
import lombok.ToString;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/27
 */
@Getter
@ToString
public class GetBrokerLiveInfoRequest implements CommandCustomHeader {

	private String clusterName;

	private String brokerName;

	private Long brokerId;

	public GetBrokerLiveInfoRequest() {
	}

	public GetBrokerLiveInfoRequest(BrokerIdentityInfo info) {
		this.clusterName = info.getClusterName();
		this.brokerName = info.getBrokerName();
		this.brokerId = info.getBrokerId();
	}

	public BrokerIdentityInfo getBrokerIdentify() {
		return new BrokerIdentityInfo(clusterName, brokerName, brokerId);
	}

	public void setBrokerIdentify(BrokerIdentityInfo info) {
		this.clusterName = info.getClusterName();
		this.brokerName = info.getBrokerName();
		this.brokerId = info.getBrokerId();
	}

	@Override
	public void checkFields() throws RemotingCommandException {
		// NOP
	}
}
