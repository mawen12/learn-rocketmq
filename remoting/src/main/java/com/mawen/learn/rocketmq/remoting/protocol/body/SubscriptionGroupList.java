package com.mawen.learn.rocketmq.remoting.protocol.body;

import java.util.List;
import java.util.concurrent.Flow;

import com.mawen.learn.rocketmq.remoting.annotation.CFNotNull;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/13
 */
public class SubscriptionGroupList extends RemotingSerializable {

	@CFNotNull
	private List<SubscriptionGroupConfig> groupConfigList;

	public SubscriptionGroupList() {
	}

	public SubscriptionGroupList(List<SubscriptionGroupConfig> groupConfigList) {
		this.groupConfigList = groupConfigList;
	}

	public List<SubscriptionGroupConfig> getGroupConfigList() {
		return groupConfigList;
	}

	public void setGroupConfigList(List<SubscriptionGroupConfig> groupConfigList) {
		this.groupConfigList = groupConfigList;
	}
}
