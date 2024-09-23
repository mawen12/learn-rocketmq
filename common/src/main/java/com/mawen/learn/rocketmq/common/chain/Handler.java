package com.mawen.learn.rocketmq.common.chain;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/9/22
 */
public interface Handler<T, R> {

	R handle(T t, HandlerChain<T, R> chain);
}
