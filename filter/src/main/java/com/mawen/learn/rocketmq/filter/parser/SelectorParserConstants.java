package com.mawen.learn.rocketmq.filter.parser;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/1
 */
public interface SelectorParserConstants {

	int EOF = 0;

	int LINE_COMMENT = 6;

	int BLOCK_COMMENT = 7;

	int NOT = 8;

	int AND = 9;

	int OR = 10;

	int BETWEEN = 11;

	int IN = 12;

	int TRUE = 13;

	int FALSE = 14;

	int NULL = 15;

	int IS = 16;

	int CONSTANTS = 17;

	int STARTSWITH = 18;

	int ENDSWITH = 19;

	int DECIMAL_LITERAL = 20;

	int FLOATING_POINT_LITERAL = 21;

	int EXPONENT = 22;

	int STRING_LITERAL = 23;

	int ID = 24;

	int DEFAULT = 0;

	String[] TOKEN_IMAGE = {
			"<EOF>",
			"\" \"",
			"\"\\t\"",
			"\"\\n\"",
			"\"\\r\"",
			"\"\\f\"",
			"<LINE_COMMENT>",
			"<BLOCK_COMMENT>",
			"\"NOT\"",
			"\"AND\"",
			"\"OR\"",
			"\"BETWEEN\"",
			"\"IN\"",
			"\"TRUE\"",
			"\"FALSE\"",
			"\"NULL\"",
			"\"IS\"",
			"\"CONTAINS\"",
			"\"STARTSWITH\"",
			"\"ENDSWITH\"",
			"<DECIMAL_LITERAL>",
			"<FLOATING_POINT_LITERAL>",
			"<EXPONENT>",
			"<STRING_LITERAL>",
			"<ID>",
			"\"=\"",
			"\"<>\"",
			"\">\"",
			"\">=\"",
			"\"<\"",
			"\"<=\"",
			"\"(\"",
			"\",\"",
			"\")\"",
			"\"+\"",
			"\"-\"",
	};

}
