package com.mawen.learn.rocketmq.remoting.rpc;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/11
 */
public abstract class TopicQueueRequestHeader extends TopicRequestHeader {

	public abstract Integer getQueueId();

	public abstract void setQueueId(Integer queueId);

}