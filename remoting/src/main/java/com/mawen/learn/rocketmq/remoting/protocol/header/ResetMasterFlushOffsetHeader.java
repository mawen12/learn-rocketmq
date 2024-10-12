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
@RocketMQAction(value = RequestCode.RESET_MASTER_FLUSH_OFFSET, resource = ResourceType.CLUSTER, action = Action.PUB)
public class ResetMasterFlushOffsetHeader implements CommandCustomHeader {

	@CFNotNull
	private Long masterFlushOffset;

	@Override
	public void checkFields() throws RemotingCommandException {

	}

	public Long getMasterFlushOffset() {
		return masterFlushOffset;
	}

	public void setMasterFlushOffset(Long masterFlushOffset) {
		this.masterFlushOffset = masterFlushOffset;
	}
}
