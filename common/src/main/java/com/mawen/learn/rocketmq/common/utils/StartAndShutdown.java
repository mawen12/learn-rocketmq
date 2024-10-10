package com.mawen.learn.rocketmq.common.utils;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/10
 */
public interface StartAndShutdown extends Start, Shutdown{

	default void preShutdown() throws Exception {}
}
