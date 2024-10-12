package com.mawen.learn.rocketmq.remoting.protocol.header.controller;

import com.mawen.learn.rocketmq.remoting.CommandCustomHeader;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
public class AlterSyncStateSetResponseHeader implements CommandCustomHeader {

	private int newSyncStateSetEpoch;

	public AlterSyncStateSetResponseHeader() {
	}

	public AlterSyncStateSetResponseHeader(int newSyncStateSetEpoch) {
		this.newSyncStateSetEpoch = newSyncStateSetEpoch;
	}

	@Override
	public void checkFields() throws RemotingCommandException {

	}

	public int getNewSyncStateSetEpoch() {
		return newSyncStateSetEpoch;
	}

	public void setNewSyncStateSetEpoch(int newSyncStateSetEpoch) {
		this.newSyncStateSetEpoch = newSyncStateSetEpoch;
	}

	@Override
	public String toString() {
		return "AlterSyncStateSetResponseHeader{" +
				"newSyncStateSetEpoch=" + newSyncStateSetEpoch +
				'}';
	}
}
