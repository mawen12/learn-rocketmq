package com.mawen.learn.rocketmq.remoting.protocol.header;

import com.mawen.learn.rocketmq.common.action.Action;
import com.mawen.learn.rocketmq.common.action.RocketMQAction;
import com.mawen.learn.rocketmq.common.resource.ResourceType;
import com.mawen.learn.rocketmq.remoting.CommandCustomHeader;
import com.mawen.learn.rocketmq.remoting.annotation.CFNotNull;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;
import com.mawen.learn.rocketmq.remoting.protocol.RequestCode;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
@RocketMQAction(value = RequestCode.VIEW_BROKER_STATS_DATA, resource = ResourceType.CLUSTER, action = Action.GET)
public class ViewBrokerStatsDataRequestHeader implements CommandCustomHeader {

	@CFNotNull
	private String statsName;

	@CFNotNull
	private String statsKey;

	@Override
	public void checkFields() throws RemotingCommandException {

	}

	public String getStatsName() {
		return statsName;
	}

	public void setStatsName(String statsName) {
		this.statsName = statsName;
	}

	public String getStatsKey() {
		return statsKey;
	}

	public void setStatsKey(String statsKey) {
		this.statsKey = statsKey;
	}
}
