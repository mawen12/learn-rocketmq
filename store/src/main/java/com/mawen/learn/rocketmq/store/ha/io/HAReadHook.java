package com.mawen.learn.rocketmq.store.ha.io;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/17
 */
public interface HAReadHook {

	void afterRead(int readSize);
}
