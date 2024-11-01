package com.mawen.learn.rocketmq.filter.expression;

import java.math.BigInteger;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/1
 */
public abstract class LogicExpression extends BinaryExpression implements BooleanExpression {

	public LogicExpression(BooleanExpression left, BooleanExpression right) {
		super(left, right);
	}

	public abstract Object evaluate(EvaluationContext context) throws Exception;

	public boolean matches(EvaluationContext context) throws Exception {
		Object value = evaluate(context);
		return value != null && value == Boolean.TRUE;
	}

	public static BooleanExpression createOR(BooleanExpression lvalue, BooleanExpression rvalue) {
		return new LogicExpression(lvalue, rvalue) {
			@Override
			public Object evaluate(EvaluationContext context) throws Exception {
				Boolean lv = (Boolean) left.evaluate(context);
				if (lv != null && lv.booleanValue()) {
					return Boolean.TRUE;
				}
				Boolean rv = (Boolean) right.evaluate(context);
				if (rv != null && rv.booleanValue()) {
					return Boolean.TRUE;
				}
				if (lv == null || rv == null) {
					return null;
				}
				return Boolean.FALSE;
			}

			@Override
			public String getExpressionSymbol() {
				return "||";
			}
		};
	}

	public static BooleanExpression createAND(BooleanExpression lvalue, BooleanExpression rvalue) {
		return new LogicExpression(lvalue, rvalue) {
			@Override
			public Object evaluate(EvaluationContext context) throws Exception {
				Boolean lv = (Boolean) left.evaluate(context);
				if (lv != null && !lv.booleanValue()) {
					return Boolean.FALSE;
				}
				Boolean rv = (Boolean) right.evaluate(context);
				if (rv != null && !rv.booleanValue()) {
					return Boolean.FALSE;
				}
				if (lv == null || rv == null) {
					return null;
				}
				return Boolean.TRUE;
			}

			@Override
			public String getExpressionSymbol() {
				return "&&";
			}
		};
	}
}
