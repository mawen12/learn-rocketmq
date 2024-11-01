package com.mawen.learn.rocketmq.filter.expression;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/1
 */
public class NowExpression extends ConstantExpression {

	public NowExpression() {
		super("now");
	}

	@Override
	public Object evaluate(EvaluationContext context) throws Exception {
		return getValue();
	}

	@Override
	public Object getValue() {
		return new Long(System.currentTimeMillis());
	}
}
