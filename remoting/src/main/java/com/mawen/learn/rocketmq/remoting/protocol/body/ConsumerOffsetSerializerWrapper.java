package com.mawen.learn.rocketmq.remoting.protocol.body;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.mawen.learn.rocketmq.remoting.protocol.DataVersion;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
public class ConsumerOffsetSerializerWrapper extends RemotingSerializable {

	private ConcurrentMap<String, ConcurrentMap<Integer, Long>> offsetTable = new ConcurrentHashMap<>();

	private DataVersion dataVersion;

	public ConcurrentMap<String, ConcurrentMap<Integer, Long>> getOffsetTable() {
		return offsetTable;
	}

	public void setOffsetTable(ConcurrentMap<String, ConcurrentMap<Integer, Long>> offsetTable) {
		this.offsetTable = offsetTable;
	}

	public DataVersion getDataVersion() {
		return dataVersion;
	}

	public void setDataVersion(DataVersion dataVersion) {
		this.dataVersion = dataVersion;
	}
}
