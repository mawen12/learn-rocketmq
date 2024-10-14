package com.mawen.learn.rocketmq.remoting.protocol.statictopic;

import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/14
 */
public class LogicQueueMappingItem extends RemotingSerializable {

	private int gen;

	private int queueId;

	private String bname;

	private long logicOffset;

	private long startOffset;

	private long endOffset = -1;

	private long timeOfStart = -1;

	private long timeOfEnd = -1;

	public LogicQueueMappingItem() {
	}

	public LogicQueueMappingItem(int gen, int queueId, String bname, long logicOffset, long startOffset, long endOffset, long timeOfStart, long timeOfEnd) {
		this.gen = gen;
		this.queueId = queueId;
		this.bname = bname;
		this.logicOffset = logicOffset;
		this.startOffset = startOffset;
		this.endOffset = endOffset;
		this.timeOfStart = timeOfStart;
		this.timeOfEnd = timeOfEnd;
	}

	public long computeStaticQueueOffsetLoosely(long physicalQueueOffset) {
		if (logicOffset < 0) {
			return -1;
		}

		if (physicalQueueOffset < startOffset) {
			return logicOffset;
		}

		if (endOffset >= startOffset && endOffset < physicalQueueOffset) {
			return logicOffset + (endOffset - startOffset);
		}

		return logicOffset + (physicalQueueOffset - startOffset);
	}

	public long computeStaticQueueOffsetStrictly(long physicalQueueOffset) {
		assert logicOffset >= 0;

		if (physicalQueueOffset < startOffset) {
			return logicOffset;
		}

		return logicOffset + (physicalQueueOffset - startOffset);
	}

	public long computePhysicalQueueOffset(long staticQueueOffset) {
		return (staticQueueOffset - logicOffset) + startOffset;
	}

	public long computeMaxStaticQueueOffset() {
		if (endOffset >= startOffset) {
			return logicOffset + endOffset - startOffset;
		}
		else {
			return logicOffset;
		}
	}

	public boolean checkIfEndOffsetDecided() {
		return endOffset > startOffset;
	}

	public boolean checkIfLogicoffsetDecided() {
		return logicOffset >= 0;
	}

	public long computeOffsetDelta() {
		return logicOffset - startOffset;
	}

	public int getGen() {
		return gen;
	}

	public void setGen(int gen) {
		this.gen = gen;
	}

	public int getQueueId() {
		return queueId;
	}

	public void setQueueId(int queueId) {
		this.queueId = queueId;
	}

	public String getBname() {
		return bname;
	}

	public void setBname(String bname) {
		this.bname = bname;
	}

	public long getLogicOffset() {
		return logicOffset;
	}

	public void setLogicOffset(long logicOffset) {
		this.logicOffset = logicOffset;
	}

	public long getStartOffset() {
		return startOffset;
	}

	public void setStartOffset(long startOffset) {
		this.startOffset = startOffset;
	}

	public long getEndOffset() {
		return endOffset;
	}

	public void setEndOffset(long endOffset) {
		this.endOffset = endOffset;
	}

	public long getTimeOfStart() {
		return timeOfStart;
	}

	public void setTimeOfStart(long timeOfStart) {
		this.timeOfStart = timeOfStart;
	}

	public long getTimeOfEnd() {
		return timeOfEnd;
	}

	public void setTimeOfEnd(long timeOfEnd) {
		this.timeOfEnd = timeOfEnd;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;

		if (!(o instanceof LogicQueueMappingItem)) return false;

		LogicQueueMappingItem item = (LogicQueueMappingItem) o;

		return new EqualsBuilder()
				.append(gen, item.gen)
				.append(queueId, item.queueId)
				.append(logicOffset, item.logicOffset)
				.append(startOffset, item.startOffset)
				.append(endOffset, item.endOffset)
				.append(timeOfStart, item.timeOfStart)
				.append(timeOfEnd, item.timeOfEnd)
				.append(bname, item.bname)
				.isEquals();
	}

	@Override
	public int hashCode() {
		return new HashCodeBuilder(17, 37)
				.append(gen)
				.append(queueId)
				.append(bname)
				.append(logicOffset)
				.append(startOffset)
				.append(endOffset)
				.append(timeOfStart)
				.append(timeOfEnd)
				.toHashCode();
	}

	@Override
	public String toString() {
		return "LogicQueueMappingItem{" +
				"gen=" + gen +
				", queueId=" + queueId +
				", bname='" + bname + '\'' +
				", logicOffset=" + logicOffset +
				", startOffset=" + startOffset +
				", endOffset=" + endOffset +
				", timeOfStart=" + timeOfStart +
				", timeOfEnd=" + timeOfEnd +
				'}';
	}
}
