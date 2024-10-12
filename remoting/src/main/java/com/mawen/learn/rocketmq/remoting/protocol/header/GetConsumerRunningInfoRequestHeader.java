package com.mawen.learn.rocketmq.remoting.protocol.header;

import com.mawen.learn.rocketmq.common.action.Action;
import com.mawen.learn.rocketmq.common.action.RocketMQAction;
import com.mawen.learn.rocketmq.common.resource.ResourceType;
import com.mawen.learn.rocketmq.common.resource.RocketMQResource;
import com.mawen.learn.rocketmq.remoting.annotation.CFNotNull;
import com.mawen.learn.rocketmq.remoting.annotation.CFNullable;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;
import com.mawen.learn.rocketmq.remoting.protocol.RequestCode;
import com.mawen.learn.rocketmq.remoting.rpc.RpcRequestHeader;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
@RocketMQAction(value = RequestCode.GET_CONSUMER_RUNNING_INFO, action = Action.GET)
public class GetConsumerRunningInfoRequestHeader extends RpcRequestHeader {

	@CFNotNull
	@RocketMQResource(ResourceType.GROUP)
	private String consumerGroup;

	@CFNotNull
	private String clientId;

	@CFNullable
	private boolean jstackEnable;

	@Override
	public void checkFields() throws RemotingCommandException {

	}

	public String getConsumerGroup() {
		return consumerGroup;
	}

	public void setConsumerGroup(String consumerGroup) {
		this.consumerGroup = consumerGroup;
	}

	public String getClientId() {
		return clientId;
	}

	public void setClientId(String clientId) {
		this.clientId = clientId;
	}

	public boolean isJstackEnable() {
		return jstackEnable;
	}

	public void setJstackEnable(boolean jstackEnable) {
		this.jstackEnable = jstackEnable;
	}

	@Override
	public String toString() {
		return "GetConsumerRunningInfoRequestHeader{" +
				"consumerGroup='" + consumerGroup + '\'' +
				", clientId='" + clientId + '\'' +
				", jstackEnable=" + jstackEnable +
				"} " + super.toString();
	}
}
