package com.mawen.learn.rocketmq.common.filter.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/7
 */
public class PolishExpr {

	public static boolean isOperand(Op token) {
		return token instanceof Operand;
	}

	public static boolean isLeftParenthesis(Op token) {
		return token instanceof Operator && Operator.LEFTPARENTHESIS == token;
	}

	public static boolean isRightParenthesis(Op token) {
		return token instanceof Operator && Operator.RIGHTPARENTHESIS == token;
	}

	public static boolean isOperator(Op token) {
		return token instanceof Operator;
	}

	public static List<Op> reversePolish(List<Op> tokens) {
		List<Op> segments = new ArrayList<>();
		Stack<Operator> operatorStack = new Stack<>();

		for (int i = 0; i < tokens.size(); i++) {
			Op op = tokens.get(i);
		}
	}

	private static List<Op> participle(String expression) {
		List<Op> segments = new ArrayList<>();

		int size = expression.length();
		int wordStartIndex = -1;
		int wordLen = 0;
		Type preType = Type.NULL;

		for (int i = 0; i < size; i++) {
			int chValue = expression.charAt(i);

			if (97 <= chValue && chValue <= 122 || 65 <= chValue && chValue <= 90 || 49 <= chValue && chValue <= 57 || 95 == chValue) {
				if (Type.OPERATOR == preType || Type.SEPAERATOR == preType || Type.NULL == preType || Type.PARENTHESIS == preType) {
					if (Type.OPERATOR == preType) {
						segments.add(Operator.createOperator(expression.substring(wordStartIndex, wordStartIndex + wordLen)));
					}
					wordStartIndex = i;
					wordLen = 0;
				}

				preType = Type.OPERAND;
				wordLen++;
			}
			else if (40 == chValue || 41 == chValue) {
				if (Type.OPERATOR == preType) {
					segments.add(Operator.createOperator(expression.substring(wordStartIndex, wordStartIndex + wordLen)));
				}
				else if (Type.OPERAND == preType) {
					segments.add(new Operand(expression.substring(wordStartIndex, wordStartIndex + wordLen)));
				}

				wordStartIndex = -1;
				wordLen = 0;

				preType = Type.PARENTHESIS;
				segments.add(Operator.createOperator((char)chValue + ""));
			}
			else if (32 == chValue || 9 == chValue) {
				if (Type.OPERATOR == preType) {
					segments.add(Operator.createOperator(expression.substring(wordStartIndex, wordStartIndex + wordLen)));
				}
				else if (Type.OPERAND == preType) {
					segments.add(new Operand(expression.substring(wordStartIndex, wordStartIndex + wordLen)));
				}

				wordStartIndex = -1;
				wordLen = 0;
				preType = Type.SEPAERATOR;
			}
			else {
				throw new IllegalArgumentException("illegal expression, at index " + i + " " + (char) chValue);
			}

		}

		if (wordLen > 0) {
			segments.add(new Operand(expression.substring(wordStartIndex, wordStartIndex + wordLen)));
		}

		return segments;
	}
}
