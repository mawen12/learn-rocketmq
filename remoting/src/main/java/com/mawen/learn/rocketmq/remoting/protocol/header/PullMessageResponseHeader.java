package com.mawen.learn.rocketmq.remoting.protocol.header;

import java.util.Map;

import com.mawen.learn.rocketmq.remoting.CommandCustomHeader;
import com.mawen.learn.rocketmq.remoting.annotation.CFNotNull;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;
import com.mawen.learn.rocketmq.remoting.protocol.FastCodesHeader;
import io.netty.buffer.ByteBuf;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/11
 */
public class PullMessageResponseHeader implements CommandCustomHeader, FastCodesHeader {

	@CFNotNull
	private long suggestWhichBrokerId;

	@CFNotNull
	private Long nextBeginOffset;

	@CFNotNull
	private Long minOffset;

	@CFNotNull
	private Long maxOffset;

	@CFNotNull
	private Long offsetDelta;

	@CFNotNull
	private Integer topicSysFlag;

	@CFNotNull
	private Integer groupSysFlag;

	@CFNotNull
	private Integer forbiddenType;

	@Override
	public void checkFields() throws RemotingCommandException {

	}

	@Override
	public void encode(ByteBuf out) {
		writeIfNotNull(out, "suggestWhichBrokerId", suggestWhichBrokerId);
		writeIfNotNull(out, "nextBeginOffset", nextBeginOffset);
		writeIfNotNull(out, "minOffset", minOffset);
		writeIfNotNull(out, "maxOffset", maxOffset);
		writeIfNotNull(out, "offsetDelta", offsetDelta);
		writeIfNotNull(out, "topicSysFlag", topicSysFlag);
		writeIfNotNull(out, "groupSysFlag", groupSysFlag);
		writeIfNotNull(out, "forbiddenType", forbiddenType);
	}

	@Override
	public void decode(Map<String, String> fields) throws RemotingCommandException {
		this.suggestWhichBrokerId = Long.parseLong(getAndCheckNotNull(fields, "suggestWhichBrokerId"));

		this.nextBeginOffset = Long.valueOf(getAndCheckNotNull(fields, "nextBeginOffset"));

		this.minOffset = Long.valueOf(getAndCheckNotNull(fields, "minOffset"));

		this.maxOffset = Long.valueOf(getAndCheckNotNull(fields, "maxOffset"));

		this.offsetDelta = Long.valueOf(getAndCheckNotNull(fields, "offsetDelta"));

		this.topicSysFlag = Integer.valueOf(getAndCheckNotNull(fields, "topicSysFlag"));

		this.groupSysFlag = Integer.valueOf(getAndCheckNotNull(fields, "groupSysFlag"));

		this.forbiddenType = Integer.valueOf(getAndCheckNotNull(fields, "forbiddenType"));
	}

	public long getSuggestWhichBrokerId() {
		return suggestWhichBrokerId;
	}

	public void setSuggestWhichBrokerId(long suggestWhichBrokerId) {
		this.suggestWhichBrokerId = suggestWhichBrokerId;
	}

	public Long getNextBeginOffset() {
		return nextBeginOffset;
	}

	public void setNextBeginOffset(Long nextBeginOffset) {
		this.nextBeginOffset = nextBeginOffset;
	}

	public Long getMinOffset() {
		return minOffset;
	}

	public void setMinOffset(Long minOffset) {
		this.minOffset = minOffset;
	}

	public Long getMaxOffset() {
		return maxOffset;
	}

	public void setMaxOffset(Long maxOffset) {
		this.maxOffset = maxOffset;
	}

	public Long getOffsetDelta() {
		return offsetDelta;
	}

	public void setOffsetDelta(Long offsetDelta) {
		this.offsetDelta = offsetDelta;
	}

	public Integer getTopicSysFlag() {
		return topicSysFlag;
	}

	public void setTopicSysFlag(Integer topicSysFlag) {
		this.topicSysFlag = topicSysFlag;
	}

	public Integer getGroupSysFlag() {
		return groupSysFlag;
	}

	public void setGroupSysFlag(Integer groupSysFlag) {
		this.groupSysFlag = groupSysFlag;
	}

	public Integer getForbiddenType() {
		return forbiddenType;
	}

	public void setForbiddenType(Integer forbiddenType) {
		this.forbiddenType = forbiddenType;
	}
}
