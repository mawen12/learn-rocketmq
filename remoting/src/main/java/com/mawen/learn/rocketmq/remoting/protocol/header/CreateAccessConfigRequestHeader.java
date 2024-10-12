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
@RocketMQAction(value = RequestCode.UPDATE_AND_CREATE_ACL_CONFIG, resource = ResourceType.CLUSTER, action = Action.UPDATE)
public class CreateAccessConfigRequestHeader implements CommandCustomHeader {

	@CFNotNull
	private String accessKey;

	private String secretKey;

	private String whiteRemoteAddress;

	private boolean admin;

	private String defaultTopicPerm;

	private String defaultGroupPerm;

	private String topicPerms;

	private String groupPerms;

	@Override
	public void checkFields() throws RemotingCommandException {

	}

	public String getAccessKey() {
		return accessKey;
	}

	public void setAccessKey(String accessKey) {
		this.accessKey = accessKey;
	}

	public String getSecretKey() {
		return secretKey;
	}

	public void setSecretKey(String secretKey) {
		this.secretKey = secretKey;
	}

	public String getWhiteRemoteAddress() {
		return whiteRemoteAddress;
	}

	public void setWhiteRemoteAddress(String whiteRemoteAddress) {
		this.whiteRemoteAddress = whiteRemoteAddress;
	}

	public boolean isAdmin() {
		return admin;
	}

	public void setAdmin(boolean admin) {
		this.admin = admin;
	}

	public String getDefaultTopicPerm() {
		return defaultTopicPerm;
	}

	public void setDefaultTopicPerm(String defaultTopicPerm) {
		this.defaultTopicPerm = defaultTopicPerm;
	}

	public String getDefaultGroupPerm() {
		return defaultGroupPerm;
	}

	public void setDefaultGroupPerm(String defaultGroupPerm) {
		this.defaultGroupPerm = defaultGroupPerm;
	}

	public String getTopicPerms() {
		return topicPerms;
	}

	public void setTopicPerms(String topicPerms) {
		this.topicPerms = topicPerms;
	}

	public String getGroupPerms() {
		return groupPerms;
	}

	public void setGroupPerms(String groupPerms) {
		this.groupPerms = groupPerms;
	}

	@Override
	public String toString() {
		return "CreateAccessConfigRequestHeader{" +
				"accessKey='" + accessKey + '\'' +
				", secretKey='" + secretKey + '\'' +
				", whiteRemoteAddress='" + whiteRemoteAddress + '\'' +
				", admin=" + admin +
				", defaultTopicPerm='" + defaultTopicPerm + '\'' +
				", defaultGroupPerm='" + defaultGroupPerm + '\'' +
				", topicPerms='" + topicPerms + '\'' +
				", groupPerms='" + groupPerms + '\'' +
				'}';
	}
}
