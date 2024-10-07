package com.mawen.learn.rocketmq.common.namesrv;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/6
 */
public interface TopAddressing {

	String fetchNSAddr();

	void registerChangeCallback(NameServerUpdateCallback changeCallback);
}
