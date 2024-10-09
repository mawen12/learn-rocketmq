package com.mawen.learn.rocketmq.common.statistics;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/9
 */
public class StatisticsKindMeta {

	private String name;

	private String[] itemNames;

	private StatisticsItemScheduledPrinter scheduledPrinter;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String[] getItemNames() {
		return itemNames;
	}

	public void setItemNames(String[] itemNames) {
		this.itemNames = itemNames;
	}

	public StatisticsItemScheduledPrinter getScheduledPrinter() {
		return scheduledPrinter;
	}

	public void setScheduledPrinter(StatisticsItemScheduledPrinter scheduledPrinter) {
		this.scheduledPrinter = scheduledPrinter;
	}
}
