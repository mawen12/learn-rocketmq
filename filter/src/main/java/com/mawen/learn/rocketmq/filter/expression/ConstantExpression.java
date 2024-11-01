package com.mawen.learn.rocketmq.filter.expression;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/1
 */
@Getter
@AllArgsConstructor
public class ConstantExpression implements Expression {

	private Object value;

	@Override
	public Object evaluate(EvaluationContext context) throws Exception {
		return value;
	}

	@Override
	public String toString() {
		Object value = getValue();
		if (value == null) {
			return "NULL";
		}
		else if (value instanceof Boolean) {
			return (Boolean) value ? "TRUE" : "FALSE";
		}
		else if (value instanceof String) {
			return encodeString((String) value);
		}
		return value.toString();
	}

	@Override
	public boolean equals(Object o) {
		if (o == null || !this.getClass().equals(o.getClass())) {
			return false;
		}
		return toString().equals(o.toString());
	}

	@Override
	public int hashCode() {
		return toString().hashCode();
	}

	public static String encodeString(String s) {
		StringBuilder sb = new StringBuilder();
		sb.append('\'');

		for (int i = 0; i < s.length(); i++) {
			char c = s.charAt(i);
			if (c == '\'') {
				sb.append(c);
			}
			sb.append(c);
		}
		sb.append('\'');

		return sb.toString();
	}

	public static ConstantExpression createFromDecimal(String text) {
		if (text.endsWith("l") || text.endsWith("L")) {
			text = text.substring(0, text.length() - 1);
		}

		Number value = new Long(text);
		long l = value.longValue();
		if (Integer.MIN_VALUE <= l && l <= Integer.MAX_VALUE) {
			value = value.intValue();
		}
		return new ConstantExpression(value);
	}

	public static ConstantExpression createFloat(String text) {
		Double value = new Double(text);
		if (value > Double.MAX_VALUE) {
			throw new RuntimeException(text + " is greater than " + Double.MAX_VALUE);
		}
		else if (value < Double.MIN_VALUE) {
			throw new RuntimeException(text + " is less than " + Double.MIN_VALUE);
		}

		return new ConstantExpression(value);
	}

	public static ConstantExpression createNow() {
		return new NowExpression();
	}
}
