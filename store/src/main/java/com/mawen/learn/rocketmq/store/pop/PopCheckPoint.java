package com.mawen.learn.rocketmq.store.pop;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/17
 */
@Setter
@Getter
@ToString
public class PopCheckPoint implements Comparable<PopCheckPoint> {

	@JSONField(name = "so")
	private long startOffset;

	@JSONField(name = "pt")
	private long popTime;

	@JSONField(name = "it")
	private long invisibleTime;

	@JSONField(name = "bm")
	private int bitMap;

	@JSONField(name = "n")
	private byte num;

	@JSONField(name = "q")
	private int queueId;

	@JSONField(name = "t")
	private String topic;

	@JSONField(name = "c")
	private String cid;

	@JSONField(name = "ro")
	private long reviveOffset;

	@JSONField(name = "d")
	private List<Integer> queueOffsetDiff;

	@JSONField(name = "bn")
	String brokerName;

	@JSONField(name = "rp")
	String retPutTimes;

	public void addDiff(int diff) {
		if (queueOffsetDiff == null) {
			queueOffsetDiff = new ArrayList<>(8);
		}

		queueOffsetDiff.add(diff);
	}

	public int indexOfAck(long ackOffset) {
		if (ackOffset < startOffset) {
			return -1;
		}

		if (queueOffsetDiff == null || queueOffsetDiff.isEmpty()) {
			if (ackOffset - startOffset < num) {
				return (int) (ackOffset - startOffset);
			}
			return -1;
		}

		return queueOffsetDiff.indexOf((int)(ackOffset - startOffset));
	}

	public long ackOffsetByIndex(byte index) {
		if (queueOffsetDiff == null || queueOffsetDiff.isEmpty()) {
			return startOffset + index;
		}

		return startOffset + queueOffsetDiff.get(index);
	}

	public int parseRePutTimes() {
		if (retPutTimes == null) {
			return 0;
		}

		try {
			return Integer.parseInt(retPutTimes);
		}
		catch (Exception ignored) {

		}
		return Byte.MAX_VALUE;
	}

	@Override
	public int compareTo(PopCheckPoint o) {
		return (int) (this.getStartOffset() - o.getStartOffset());
	}
}
