package com.mawen.learn.rocketmq.common.state;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/9
 */
public interface StateEventListener<T> {

	void fireEvent(T event);
}
