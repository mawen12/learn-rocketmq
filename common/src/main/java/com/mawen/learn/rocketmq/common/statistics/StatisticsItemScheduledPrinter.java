package com.mawen.learn.rocketmq.common.statistics;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/9
 */
public class StatisticsItemScheduledPrinter extends FutureHolder {

	protected String name;
	protected StatisticsItemPrinter printer;
	protected ScheduledExecutorService executor;
	protected long interval;
	protected InitialDelay initialDelay;
	protected Valve valve;

	public StatisticsItemScheduledPrinter(String name, StatisticsItemPrinter printer, ScheduledExecutorService executor, InitialDelay initialDelay, long interval,  Valve valve) {
		this.name = name;
		this.printer = printer;
		this.executor = executor;
		this.interval = interval;
		this.initialDelay = initialDelay;
		this.valve = valve;
	}

	public void schedule(StatisticsItem item) {
		ScheduledFuture<?> future = executor.scheduleAtFixedRate(() -> {
			if (enabled()) {
				printer.print(name, item);
			}
		}, getInitialDelay(), interval, TimeUnit.MILLISECONDS);

		addFuture(item, future);
	}

	public void remove(StatisticsItem item) {
		removeAllFuture(item);
	}

	protected long getInitialDelay() {
		return initialDelay != null ? initialDelay.get() : 0;
	}

	protected boolean enabled() {
		return valve != null ? valve.enabled() : false;
	}

	protected boolean printZeroLine() {
		return valve != null ? valve.printZeroLine() : false;
	}

	public interface InitialDelay {
		long get();
	}

	public interface Valve {
		boolean enabled();

		boolean printZeroLine();
	}
}
