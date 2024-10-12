package com.mawen.learn.rocketmq.remoting.protocol.header.controller;

import com.mawen.learn.rocketmq.remoting.CommandCustomHeader;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
public class GetReplicaInfoResponseHeader implements CommandCustomHeader {

	private Long masterBrokerId;

	private String masterAddress;

	private Integer masterEpoch;

	public GetReplicaInfoResponseHeader() {
	}

	@Override
	public void checkFields() throws RemotingCommandException {

	}

	public Long getMasterBrokerId() {
		return masterBrokerId;
	}

	public void setMasterBrokerId(Long masterBrokerId) {
		this.masterBrokerId = masterBrokerId;
	}

	public String getMasterAddress() {
		return masterAddress;
	}

	public void setMasterAddress(String masterAddress) {
		this.masterAddress = masterAddress;
	}

	public Integer getMasterEpoch() {
		return masterEpoch;
	}

	public void setMasterEpoch(Integer masterEpoch) {
		this.masterEpoch = masterEpoch;
	}

	@Override
	public String toString() {
		return "GetReplicaInfoResponseHeader{" +
				"masterBrokerId=" + masterBrokerId +
				", masterAddress='" + masterAddress + '\'' +
				", masterEpoch=" + masterEpoch +
				'}';
	}
}
