package com.mawen.learn.rocketmq.filter.expression;

import java.util.Collection;
import java.util.stream.Collectors;

import com.mawen.learn.rocketmq.filter.constant.UnaryType;
import lombok.Getter;
import lombok.Setter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/1
 */
@Getter
@Setter
public abstract class UnaryInExpression extends UnaryExpression implements BooleanExpression {

	private boolean not;

	private Collection inList;

	public UnaryInExpression(Expression left, UnaryType unaryType, Collection inList, boolean not) {
		super(left, unaryType);
		this.inList = inList;
		this.not = not;
	}

	@Override
	public boolean matches(EvaluationContext context) throws Exception {
		Object value = evaluate(context);
		return value != null && value == Boolean.TRUE;
	}

	@Override
	public String getExpressionSymbol() {
		return not ? "NOT IN" : "IN";
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(right);
		sb.append(" ");
		sb.append(getExpressionSymbol());
		sb.append(" ( ");

		sb.append(inList.stream().collect(Collectors.joining(", ")));
		sb.append(" ) ");
		return sb.toString();
	}
}
