package com.mawen.learn.rocketmq.store.queue;

import java.util.Iterator;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/5
 */
public interface ReferredIterator<T> extends Iterator<T> {

	void release();

	T nextAndRelease();
}
