package com.mawen.learn.rocketmq.remoting.protocol.body;

import java.util.HashMap;
import java.util.Map;

import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/13
 */
public class QueryCorrectionOffsetBody extends RemotingSerializable {

	private Map<Integer, Long> correctionOffsets = new HashMap<>();

	public Map<Integer, Long> getCorrectionOffsets() {
		return correctionOffsets;
	}

	public void setCorrectionOffsets(Map<Integer, Long> correctionOffsets) {
		this.correctionOffsets = correctionOffsets;
	}
}
