package com.mawen.learn.rocketmq.remoting.rpc;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/11
 */
public abstract class TopicRequestHeader extends RpcRequestHeader{

	protected Boolean lo;

	public abstract String getTopic();

	public abstract void setTopic(String topic);

	public Boolean getLo() {
		return lo;
	}

	public void setLo(Boolean lo) {
		this.lo = lo;
	}
}
