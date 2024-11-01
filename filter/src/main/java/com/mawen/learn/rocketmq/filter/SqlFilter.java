package com.mawen.learn.rocketmq.filter;

import com.mawen.learn.rocketmq.common.filter.ExpressionType;
import com.mawen.learn.rocketmq.filter.expression.Expression;
import com.mawen.learn.rocketmq.filter.expression.MQFilterException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/1
 */
public class SqlFilter implements FilterSpi {

	@Override
	public Expression compile(String expr) throws MQFilterException {
		return SelectorParser.parse(expr);
	}

	@Override
	public String ofType() {
		return ExpressionType.SQL92;
	}
}
