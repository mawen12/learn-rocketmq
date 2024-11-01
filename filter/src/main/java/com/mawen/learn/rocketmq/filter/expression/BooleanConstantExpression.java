package com.mawen.learn.rocketmq.filter.expression;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/1
 */
public class BooleanConstantExpression extends ConstantExpression implements BooleanExpression{

	public static final BooleanConstantExpression NULL = new BooleanConstantExpression(null);
	public static final BooleanConstantExpression TRUE = new BooleanConstantExpression(Boolean.TRUE);
	public static final BooleanConstantExpression FALSE = new BooleanConstantExpression(Boolean.FALSE);

	public BooleanConstantExpression(Object value) {
		super(value);
	}

	@Override
	public boolean matches(EvaluationContext context) throws Exception {
		Object value = evaluate(context);
		return value != null && value == Boolean.TRUE;
	}
}
