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
@RocketMQAction(value = RequestCode.UPDATE_GLOBAL_WHITE_ADDRS_CONFIG, resource = ResourceType.CLUSTER, action = Action.UPDATE)
public class UpdateGlobalWhiteAddrConfigRequestHeader implements CommandCustomHeader {

	@CFNotNull
	private String globalWhiteAddrs;

	@CFNotNull
	private String aclFileFullPath;

	@Override
	public void checkFields() throws RemotingCommandException {

	}

	public String getGlobalWhiteAddrs() {
		return globalWhiteAddrs;
	}

	public void setGlobalWhiteAddrs(String globalWhiteAddrs) {
		this.globalWhiteAddrs = globalWhiteAddrs;
	}

	public String getAclFileFullPath() {
		return aclFileFullPath;
	}

	public void setAclFileFullPath(String aclFileFullPath) {
		this.aclFileFullPath = aclFileFullPath;
	}
}
