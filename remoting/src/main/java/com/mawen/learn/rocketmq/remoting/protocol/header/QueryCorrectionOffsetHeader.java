package com.mawen.learn.rocketmq.remoting.protocol.header;

import com.mawen.learn.rocketmq.common.action.Action;
import com.mawen.learn.rocketmq.common.action.RocketMQAction;
import com.mawen.learn.rocketmq.common.resource.ResourceType;
import com.mawen.learn.rocketmq.common.resource.RocketMQResource;
import com.mawen.learn.rocketmq.remoting.annotation.CFNotNull;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;
import com.mawen.learn.rocketmq.remoting.protocol.RequestCode;
import com.mawen.learn.rocketmq.remoting.rpc.TopicRequestHeader;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
@RocketMQAction(value = RequestCode.QUERY_CORRECTION_OFFSET, action = Action.GET)
public class QueryCorrectionOffsetHeader extends TopicRequestHeader {

	@RocketMQResource(value = ResourceType.GROUP, splitter = ",")
	private String filterGroups;

	@CFNotNull
	@RocketMQResource(ResourceType.GROUP)
	private String compareGroup;

	@CFNotNull
	@RocketMQResource(ResourceType.TOPIC)
	private String topic;

	@Override
	public void checkFields() throws RemotingCommandException {

	}

	public String getFilterGroups() {
		return filterGroups;
	}

	public void setFilterGroups(String filterGroups) {
		this.filterGroups = filterGroups;
	}

	public String getCompareGroup() {
		return compareGroup;
	}

	public void setCompareGroup(String compareGroup) {
		this.compareGroup = compareGroup;
	}

	@Override
	public String getTopic() {
		return topic;
	}

	@Override
	public void setTopic(String topic) {
		this.topic = topic;
	}
}
