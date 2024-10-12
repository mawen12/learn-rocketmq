package com.mawen.learn.rocketmq.remoting.protocol.header.controller;

import com.mawen.learn.rocketmq.remoting.CommandCustomHeader;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
public class GetMetadataResponseHeader implements CommandCustomHeader {

	private String group;

	private String controllerLeaderId;

	private String controllerLeaderAddress;

	private boolean isLeader;

	private String peers;

	public GetMetadataResponseHeader() {
	}

	public GetMetadataResponseHeader(String group, String controllerLeaderId, String controllerLeaderAddress, boolean isLeader, String peers) {
		this.group = group;
		this.controllerLeaderId = controllerLeaderId;
		this.controllerLeaderAddress = controllerLeaderAddress;
		this.isLeader = isLeader;
		this.peers = peers;
	}

	@Override
	public void checkFields() throws RemotingCommandException {

	}

	public String getGroup() {
		return group;
	}

	public void setGroup(String group) {
		this.group = group;
	}

	public String getControllerLeaderId() {
		return controllerLeaderId;
	}

	public void setControllerLeaderId(String controllerLeaderId) {
		this.controllerLeaderId = controllerLeaderId;
	}

	public String getControllerLeaderAddress() {
		return controllerLeaderAddress;
	}

	public void setControllerLeaderAddress(String controllerLeaderAddress) {
		this.controllerLeaderAddress = controllerLeaderAddress;
	}

	public boolean isLeader() {
		return isLeader;
	}

	public void setLeader(boolean leader) {
		isLeader = leader;
	}

	public String getPeers() {
		return peers;
	}

	public void setPeers(String peers) {
		this.peers = peers;
	}

	@Override
	public String toString() {
		return "GetMetadataResponseHeader{" +
				"group='" + group + '\'' +
				", controllerLeaderId='" + controllerLeaderId + '\'' +
				", controllerLeaderAddress='" + controllerLeaderAddress + '\'' +
				", isLeader=" + isLeader +
				", peers='" + peers + '\'' +
				'}';
	}
}
