package com.mawen.learn.rocketmq.remoting.protocol.header;

import com.mawen.learn.rocketmq.common.action.Action;
import com.mawen.learn.rocketmq.common.action.RocketMQAction;
import com.mawen.learn.rocketmq.common.resource.ResourceType;
import com.mawen.learn.rocketmq.common.resource.RocketMQResource;
import com.mawen.learn.rocketmq.remoting.annotation.CFNotNull;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;
import com.mawen.learn.rocketmq.remoting.protocol.RequestCode;
import com.mawen.learn.rocketmq.remoting.rpc.RpcRequestHeader;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
@RocketMQAction(value = RequestCode.CLONE_GROUP_OFFSET, action = Action.UPDATE)
public class CloneGroupOffsetRequestHeader extends RpcRequestHeader {

	@CFNotNull
	private String srcGroup;

	@CFNotNull
	@RocketMQResource(ResourceType.GROUP)
	private String destGroup;

	@RocketMQResource(ResourceType.TOPIC)
	private String topic;

	private boolean offline;

	@Override
	public void checkFields() throws RemotingCommandException {

	}

	public String getSrcGroup() {
		return srcGroup;
	}

	public void setSrcGroup(String srcGroup) {
		this.srcGroup = srcGroup;
	}

	public String getDestGroup() {
		return destGroup;
	}

	public void setDestGroup(String destGroup) {
		this.destGroup = destGroup;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public boolean isOffline() {
		return offline;
	}

	public void setOffline(boolean offline) {
		this.offline = offline;
	}

	@Override
	public String toString() {
		return "CloneGroupOffsetRequestHeader{" +
				"srcGroup='" + srcGroup + '\'' +
				", destGroup='" + destGroup + '\'' +
				", topic='" + topic + '\'' +
				", offline=" + offline +
				"} " + super.toString();
	}
}
