package com.mawen.learn.rocketmq.common.message;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/4
 */
public class MessageClientExt extends MessageExt{

	public String getOffsetMsgId() {
		return super.getMsgId();
	}

	public void setOffsetMsgId(String offsetMsgId) {
		super.setMsgId(offsetMsgId);
	}

	@Override
	public String getMsgId() {
		String uniqID = MessageClientIDSetter.getUniqID(this);
		if (uniqID == null) {
			return this.getOffsetMsgId();
		}
		else {
			return uniqID;
		}
	}

	@Override
	public void setMsgId(String msgId) {
		// NOP
	}
}
