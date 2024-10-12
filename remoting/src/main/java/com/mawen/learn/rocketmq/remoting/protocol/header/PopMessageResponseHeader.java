package com.mawen.learn.rocketmq.remoting.protocol.header;

import com.mawen.learn.rocketmq.remoting.CommandCustomHeader;
import com.mawen.learn.rocketmq.remoting.annotation.CFNotNull;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
public class PopMessageResponseHeader implements CommandCustomHeader {

	@CFNotNull
	private long popTime;

	@CFNotNull
	private long invisibleTime;

	@CFNotNull
	private int reviveQid;

	@CFNotNull
	private long restNum;

	private String startOffsetInfo;

	private String msgOffsetInfo;

	private String orderCountInfo;

	@Override
	public void checkFields() throws RemotingCommandException {

	}

	public long getPopTime() {
		return popTime;
	}

	public void setPopTime(long popTime) {
		this.popTime = popTime;
	}

	public long getInvisibleTime() {
		return invisibleTime;
	}

	public void setInvisibleTime(long invisibleTime) {
		this.invisibleTime = invisibleTime;
	}

	public int getReviveQid() {
		return reviveQid;
	}

	public void setReviveQid(int reviveQid) {
		this.reviveQid = reviveQid;
	}

	public long getRestNum() {
		return restNum;
	}

	public void setRestNum(long restNum) {
		this.restNum = restNum;
	}

	public String getStartOffsetInfo() {
		return startOffsetInfo;
	}

	public void setStartOffsetInfo(String startOffsetInfo) {
		this.startOffsetInfo = startOffsetInfo;
	}

	public String getMsgOffsetInfo() {
		return msgOffsetInfo;
	}

	public void setMsgOffsetInfo(String msgOffsetInfo) {
		this.msgOffsetInfo = msgOffsetInfo;
	}

	public String getOrderCountInfo() {
		return orderCountInfo;
	}

	public void setOrderCountInfo(String orderCountInfo) {
		this.orderCountInfo = orderCountInfo;
	}
}
