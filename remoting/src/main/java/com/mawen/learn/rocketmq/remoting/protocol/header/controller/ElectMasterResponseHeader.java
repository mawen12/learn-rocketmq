package com.mawen.learn.rocketmq.remoting.protocol.header.controller;

import com.mawen.learn.rocketmq.remoting.CommandCustomHeader;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
public class ElectMasterResponseHeader implements CommandCustomHeader {

	private Long masterBrokerId;

	private String masterAddress;

	private Integer masterEpoch;

	private Integer syncStateSetEpoch;

	public ElectMasterResponseHeader() {
	}

	public ElectMasterResponseHeader(Long masterBrokerId, String masterAddress, Integer masterEpoch, Integer syncStateSetEpoch) {
		this.masterBrokerId = masterBrokerId;
		this.masterAddress = masterAddress;
		this.masterEpoch = masterEpoch;
		this.syncStateSetEpoch = syncStateSetEpoch;
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

	public Integer getSyncStateSetEpoch() {
		return syncStateSetEpoch;
	}

	public void setSyncStateSetEpoch(Integer syncStateSetEpoch) {
		this.syncStateSetEpoch = syncStateSetEpoch;
	}

	@Override
	public String toString() {
		return "ElectMasterResponseHeader{" +
				"masterBrokerId=" + masterBrokerId +
				", masterAddress='" + masterAddress + '\'' +
				", masterEpoch=" + masterEpoch +
				", syncStateSetEpoch=" + syncStateSetEpoch +
				'}';
	}

	@Override
	public void checkFields() throws RemotingCommandException {

	}
}
