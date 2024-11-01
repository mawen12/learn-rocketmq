package com.mawen.learn.rocketmq.filter.expression;

import java.util.Objects;

import javassist.expr.Expr;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/1
 */
@Setter
@Getter
@AllArgsConstructor
public abstract class BinaryExpression implements Expression {

	protected Expression left;
	protected Expression right;

	public abstract String getExpressionSymbol();

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

	@Override
	public String toString() {
		return "(" + left.toString() + " " + getExpressionSymbol() + " " + right.toString() + ")";
	}
}
