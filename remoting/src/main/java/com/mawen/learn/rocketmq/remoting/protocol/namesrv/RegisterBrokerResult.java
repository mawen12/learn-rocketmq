package com.mawen.learn.rocketmq.remoting.protocol.namesrv;

import com.mawen.learn.rocketmq.remoting.protocol.body.KVTable;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/13
 */
public class RegisterBrokerResult {

	private String haServerAddr;

	private String masterAddr;

	private KVTable kvTable;

	public String getHaServerAddr() {
		return haServerAddr;
	}

	public void setHaServerAddr(String haServerAddr) {
		this.haServerAddr = haServerAddr;
	}

	public String getMasterAddr() {
		return masterAddr;
	}

	public void setMasterAddr(String masterAddr) {
		this.masterAddr = masterAddr;
	}

	public KVTable getKvTable() {
		return kvTable;
	}

	public void setKvTable(KVTable kvTable) {
		this.kvTable = kvTable;
	}
}
