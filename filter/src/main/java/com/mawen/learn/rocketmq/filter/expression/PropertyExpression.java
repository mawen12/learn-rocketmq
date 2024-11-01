package com.mawen.learn.rocketmq.filter.expression;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/1
 */
@Getter
@RequiredArgsConstructor
public class PropertyExpression implements Expression {

	private final String name;

	@Override
	public Object evaluate(EvaluationContext context) throws Exception {
		return context.get(name);
	}

	@Override
	public int hashCode() {
		return name.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (o == null || !this.getClass().equals(o.getClass())) {
			return false;
		}
		return name.equals(((PropertyExpression) o).name);
	}

	@Override
	public String toString() {
		return name;
	}
}
