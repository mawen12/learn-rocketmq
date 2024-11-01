package com.mawen.learn.rocketmq.filter;

import com.mawen.learn.rocketmq.filter.expression.Expression;
import com.mawen.learn.rocketmq.filter.expression.MQFilterException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/1
 */
public interface FilterSpi {

	Expression compile(final String expr) throws MQFilterException;

	String ofType();
}
