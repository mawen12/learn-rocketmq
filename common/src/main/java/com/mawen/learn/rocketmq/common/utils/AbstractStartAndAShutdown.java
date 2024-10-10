package com.mawen.learn.rocketmq.common.utils;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/10
 */
public abstract class AbstractStartAndAShutdown implements StartAndShutdown {

	protected List<StartAndShutdown> startAndShutdownList = new CopyOnWriteArrayList<>();

	protected void appendStartAndShutdown(StartAndShutdown startAndShutdown) {
		this.startAndShutdownList.add(startAndShutdown);
	}

	@Override
	public void start() throws Exception {
		for (StartAndShutdown startAndShutdown : startAndShutdownList) {
			startAndShutdown.start();
		}
	}

	@Override
	public void shutdown() throws Exception {
		int index = startAndShutdownList.size() - 1;
		for (; index >= 0; index--) {
			startAndShutdownList.get(index).shutdown();
		}
	}

	@Override
	public void preShutdown() throws Exception {
		int index = startAndShutdownList.size() - 1;
		for (; index >= 0; index--) {
			startAndShutdownList.get(index).preShutdown();
		}
	}

	public void appendStart(Start start) {
		this.appendStartAndShutdown(new StartAndShutdown() {
			@Override
			public void shutdown() throws Exception {

			}

			@Override
			public void start() throws Exception {
				start.start();
			}
		});
	}

	public void appendShutdown(Shutdown shutdown) {
		this.appendStartAndShutdown(new StartAndShutdown() {
			@Override
			public void shutdown() throws Exception {
				shutdown.shutdown();
			}

			@Override
			public void start() throws Exception {

			}
		});
	}
}
