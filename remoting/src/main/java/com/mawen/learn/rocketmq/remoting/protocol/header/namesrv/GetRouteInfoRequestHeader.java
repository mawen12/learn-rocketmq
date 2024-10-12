package com.mawen.learn.rocketmq.remoting.protocol.header.namesrv;

import com.mawen.learn.rocketmq.common.action.Action;
import com.mawen.learn.rocketmq.common.action.RocketMQAction;
import com.mawen.learn.rocketmq.common.resource.ResourceType;
import com.mawen.learn.rocketmq.remoting.annotation.CFNotNull;
import com.mawen.learn.rocketmq.remoting.annotation.CFNullable;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;
import com.mawen.learn.rocketmq.remoting.protocol.RequestCode;
import com.mawen.learn.rocketmq.remoting.rpc.TopicRequestHeader;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
@RocketMQAction(value = RequestCode.GET_ROUTEINFO_BY_TOPIC, resource = ResourceType.CLUSTER, action = Action.GET)
public class GetRouteInfoRequestHeader extends TopicRequestHeader {

	@CFNotNull
	private String topic;

	@CFNullable
	private Boolean acceptStandardJsonOnly;

	@Override
	public void checkFields() throws RemotingCommandException {

	}

	@Override
	public String getTopic() {
		return topic;
	}

	@Override
	public void setTopic(String topic) {
		this.topic = topic;
	}

	public Boolean getAcceptStandardJsonOnly() {
		return acceptStandardJsonOnly;
	}

	public void setAcceptStandardJsonOnly(Boolean acceptStandardJsonOnly) {
		this.acceptStandardJsonOnly = acceptStandardJsonOnly;
	}
}
