package com.mawen.learn.rocketmq.remoting.protocol.header;

import com.mawen.learn.rocketmq.common.action.Action;
import com.mawen.learn.rocketmq.common.action.RocketMQAction;
import com.mawen.learn.rocketmq.common.resource.ResourceType;
import com.mawen.learn.rocketmq.remoting.CommandCustomHeader;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;
import com.mawen.learn.rocketmq.remoting.protocol.RequestCode;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
@RocketMQAction(value = RequestCode.AUTH_LIST_ACL, resource = ResourceType.CLUSTER, action = Action.GET)
public class ListAclsRequestHeader implements CommandCustomHeader {

	private String subjectFilter;

	private String resourceFilter;

	public ListAclsRequestHeader() {
	}

	public ListAclsRequestHeader(String subjectFilter, String resourceFilter) {
		this.subjectFilter = subjectFilter;
		this.resourceFilter = resourceFilter;
	}

	@Override
	public void checkFields() throws RemotingCommandException {

	}

	public String getSubjectFilter() {
		return subjectFilter;
	}

	public void setSubjectFilter(String subjectFilter) {
		this.subjectFilter = subjectFilter;
	}

	public String getResourceFilter() {
		return resourceFilter;
	}

	public void setResourceFilter(String resourceFilter) {
		this.resourceFilter = resourceFilter;
	}
}
