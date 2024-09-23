package com.mawen.learn.rocketmq.common.chain;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/9/22
 */
public class HandlerChain<T, R> {

	private List<Handler<T, R>> handlers;
	private Iterator<Handler<T, R>> iterator;

	public static <T, R> HandlerChain<T, R> create() {
		return new HandlerChain<>();
	}

	public HandlerChain<T, R> addNext(Handler<T, R> handler) {
		if (this.handlers == null) {
			this.handlers = new ArrayList<>();
		}
		this.handlers.add(handler);
		return this;
	}

	public R handle(T t) {
		if (this.iterator == null) {
			this.iterator = handlers.iterator();
		}

		if (this.iterator.hasNext()) {
			Handler<T, R> handler = iterator.next();
			return handler.handle(t, this);
		}
		return null;
	}
}
