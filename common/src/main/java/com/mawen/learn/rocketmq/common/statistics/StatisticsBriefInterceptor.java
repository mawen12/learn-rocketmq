package com.mawen.learn.rocketmq.common.statistics;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/9
 */
public class StatisticsBriefInterceptor implements Interceptor{

	private int[] indexOfItems;

	private StatisticsBrief[] statisticsBriefs;

	public StatisticsBriefInterceptor(StatisticsItem item, Pair<String, long[][]>[] briefMetas) {
		this.indexOfItems = new int[briefMetas.length];
		this.statisticsBriefs = new StatisticsBrief[briefMetas.length];

		for (int i = 0; i < briefMetas.length; i++) {
			String name = briefMetas[i].getKey();
			int index = ArrayUtils.indexOf(item.getItemNames(), name);
			if (index < 0) {
				throw new IllegalArgumentException("illegal briefItemName: " + name);
			}
			indexOfItems[i] = index;
			statisticsBriefs[i] = new StatisticsBrief(briefMetas[i].getValue());
		}
	}

	@Override
	public void inc(long... itemValues) {
		for (int i = 0; i < indexOfItems.length; i++) {
			int indexOfItem = indexOfItems[i];
			if (indexOfItem < itemValues.length) {
				statisticsBriefs[i].sample(itemValues[indexOfItem]);
			}
		}
	}

	@Override
	public void reset() {
		for (StatisticsBrief brief : statisticsBriefs) {
			brief.reset();
		}
	}

	public int[] getIndexOfItems() {
		return indexOfItems;
	}

	public void setIndexOfItems(int[] indexOfItems) {
		this.indexOfItems = indexOfItems;
	}

	public StatisticsBrief[] getStatisticsBriefs() {
		return statisticsBriefs;
	}

	public void setStatisticsBriefs(StatisticsBrief[] statisticsBriefs) {
		this.statisticsBriefs = statisticsBriefs;
	}
}
