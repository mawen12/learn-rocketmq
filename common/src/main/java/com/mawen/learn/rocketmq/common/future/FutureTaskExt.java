package com.mawen.learn.rocketmq.common.future;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/9
 */
public class FutureTaskExt<V> extends FutureTask<V> {

	private final Runnable runnable;

	public FutureTaskExt(Callable<V> callable) {
		super(callable);
		this.runnable = null;
	}

	public FutureTaskExt(Runnable runnable, V result) {
		super(runnable, result);
		this.runnable = runnable;
	}

	public Runnable getRunnable() {
		return runnable;
	}
}
