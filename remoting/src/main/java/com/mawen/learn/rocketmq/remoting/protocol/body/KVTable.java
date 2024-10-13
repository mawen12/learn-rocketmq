package com.mawen.learn.rocketmq.remoting.protocol.body;

import java.util.HashMap;
import java.util.Map;

import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/13
 */
public class KVTable extends RemotingSerializable {

	private Map<String, String> table = new HashMap<>();

	public Map<String, String> getTable() {
		return table;
	}

	public void setTable(Map<String, String> table) {
		this.table = table;
	}
}
