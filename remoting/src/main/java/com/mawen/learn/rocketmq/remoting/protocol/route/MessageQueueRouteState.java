package com.mawen.learn.rocketmq.remoting.protocol.route;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/13
 */
public enum MessageQueueRouteState {

	Expired,

	ReadOnly,

	Normal,

	WriteOnly
}
