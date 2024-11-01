package com.mawen.learn.rocketmq.filter.expression;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import com.mawen.learn.rocketmq.filter.constant.UnaryType;
import lombok.Getter;
import lombok.Setter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/1
 */
@Getter
@Setter
public abstract class UnaryExpression implements Expression {

	private static final BigDecimal BD_LONG_MIN_VALUE = BigDecimal.valueOf(Long.MIN_VALUE);

	protected Expression right;

	public UnaryType unaryType;

	public UnaryExpression(Expression left) {
		this.right = left;
	}

	public UnaryExpression(Expression left, UnaryType unaryType) {
		this.right = left;
		this.unaryType = unaryType;
	}

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
		return "(" + getExpressionSymbol() + " " + right.toString() + ")";
	}

	public static Expression createNegate(Expression left) {
		return new UnaryExpression(left, UnaryType.NEGATE) {

			@Override
			public Object evaluate(EvaluationContext context) throws Exception {
				Object rvalue = right.evaluate(context);
				if (rvalue == null) {
					return null;
				}
				else if (rvalue instanceof Number) {
					return negate((Number) rvalue);
				}
				return null;
			}

			@Override
			public String getExpressionSymbol() {
				return "-";
			}
		};
	}

	abstract static class BooleanUnaryExpression extends UnaryExpression implements BooleanExpression {
		public BooleanUnaryExpression(Expression left, UnaryType unaryType) {
			super(left, unaryType);
		}

		@Override
		public boolean matches(EvaluationContext context) throws Exception {
			Object value = evaluate(context);
			return value != null && value == Boolean.TRUE;
		}
	}

	public static BooleanExpression createNot(BooleanExpression left) {
		return new BooleanUnaryExpression(left, UnaryType.NOT) {
			@Override
			public String getExpressionSymbol() {
				return "NOT";
			}

			@Override
			public Object evaluate(EvaluationContext context) throws Exception {
				Boolean lvalue = (Boolean) right.evaluate(context);
				if (lvalue == null) {
					return null;
				}
				return lvalue.booleanValue() ? Boolean.FALSE : Boolean.TRUE;
			}
		};
	}

	public static BooleanExpression createBooleanCast(Expression left) {
		return new BooleanUnaryExpression(left, UnaryType.BOOLEANCAST) {
			@Override
			public String getExpressionSymbol() {
				return "";
			}

			@Override
			public Object evaluate(EvaluationContext context) throws Exception {
				Object rvalue = right.evaluate(context);
				if (rvalue == null) {
					return null;
				}
				else if (rvalue.getClass().equals(Boolean.class)) {
					return Boolean.FALSE;
				}
				return ((Boolean) rvalue).booleanValue() ? Boolean.TRUE : Boolean.FALSE;
			}

			@Override
			public String toString() {
				return right.toString();
			}
		};
	}

	public static BooleanExpression createInExpression(PropertyExpression right, List<Object> elements, final boolean not) {
		Collection<Object> t;
		if (elements.size() == 0) {
			t = null;
		}
		else if (elements.size() < 5) {
			t = elements;
		}
		else {
			t = new HashSet<>(elements);
		}

		final Collection inList = t;

		return new UnaryInExpression(right, UnaryType.IN, inList, not) {

			@Override
			public Object evaluate(EvaluationContext context) throws Exception {
				Object rvalue = right.evaluate(context);
				if (rvalue == null) {
					return null;
				}
				else if (rvalue.getClass() != String.class) {
					return null;
				}

				return (inList != null && inList.contains(rvalue)) ^ not;
			}
		};
	}

	private static Number negate(Number left) {
		Class<? extends Number> clazz = left.getClass();
		if (clazz == Integer.class) {
			return new Integer(-left.intValue());
		}
		else if (clazz == Long.class) {
			return new Long(-left.longValue());
		}
		else if (clazz == Float.class) {
			return new Float(-left.floatValue());
		}
		else if (clazz == Double.class) {
			return new Double(-left.doubleValue());
		}
		else if (clazz == BigDecimal.class) {
			BigDecimal bd = (BigDecimal) left;
			bd = bd.negate();

			if (BD_LONG_MIN_VALUE.compareTo(bd) == 0) {
				return Long.valueOf(Long.MIN_VALUE);
			}
			return bd;
		}
		else {
			throw new RuntimeException("Don't known how to negate: " + left);
		}
	}
}
