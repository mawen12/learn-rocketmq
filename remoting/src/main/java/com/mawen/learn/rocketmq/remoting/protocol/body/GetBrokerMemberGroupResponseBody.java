package com.mawen.learn.rocketmq.remoting.protocol.body;

import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/13
 */
public class GetBrokerMemberGroupResponseBody extends RemotingSerializable {

	private BrokerMemberGroup brokerMemberGroup;

	public BrokerMemberGroup getBrokerMemberGroup() {
		return brokerMemberGroup;
	}

	public void setBrokerMemberGroup(BrokerMemberGroup brokerMemberGroup) {
		this.brokerMemberGroup = brokerMemberGroup;
	}
}
