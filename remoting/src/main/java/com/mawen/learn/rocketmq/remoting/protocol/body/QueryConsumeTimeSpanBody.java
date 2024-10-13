package com.mawen.learn.rocketmq.remoting.protocol.body;

import java.util.ArrayList;
import java.util.List;

import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/13
 */
public class QueryConsumeTimeSpanBody extends RemotingSerializable {

	private List<QueueTimeSpan> consumeTimeSpanSet = new ArrayList<>();

	public List<QueueTimeSpan> getConsumeTimeSpanSet() {
		return consumeTimeSpanSet;
	}

	public void setConsumeTimeSpanSet(List<QueueTimeSpan> consumeTimeSpanSet) {
		this.consumeTimeSpanSet = consumeTimeSpanSet;
	}
}
