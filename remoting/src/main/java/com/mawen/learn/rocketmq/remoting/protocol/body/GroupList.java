package com.mawen.learn.rocketmq.remoting.protocol.body;

import java.util.HashSet;
import java.util.Set;

import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/13
 */
public class GroupList extends RemotingSerializable {

	private Set<String> groupList = new HashSet<>();

	public Set<String> getGroupList() {
		return groupList;
	}

	public void setGroupList(Set<String> groupList) {
		this.groupList = groupList;
	}
}
