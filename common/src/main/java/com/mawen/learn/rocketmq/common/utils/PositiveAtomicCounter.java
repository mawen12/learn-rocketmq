package com.mawen.learn.rocketmq.common.utils;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/10
 */
public class PositiveAtomicCounter {

	private static final int MASK = 0x7FFFFFFF;

	private final AtomicInteger atom;

	public PositiveAtomicCounter() {
		this.atom = new AtomicInteger(0);
	}

	public final int incrementAndGet() {
		int rt = atom.incrementAndGet();
		return rt & MASK;
	}

	public int intValue() {
		return atom.intValue();
	}
}
