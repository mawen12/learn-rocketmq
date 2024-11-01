package com.mawen.learn.rocketmq.filter.parser;

import lombok.Getter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/1
 */
@Getter
public class TokenMgrError extends Error {

	private static final long serialVersionUID = 1L;

	static final int LEXICAL_ERROR = 0;

	static final int STATIC_LEXER_ERROR = 1;

	static final int INVALID_LEXICAL_ERROR = 2;

	static final int LOOP_DETECTED = 3;

	int errorCode;

	public TokenMgrError() {
	}

	public TokenMgrError(String message, int reason) {
		super(message);
		errorCode = reason;
	}

	public TokenMgrError(boolean eofSeen, int lexState, int errorLine, int errorColumn, String errorAfter, char curChar, int reason) {
		this(lexicalError(eofSeen, lexState, errorLine, errorColumn, errorAfter, curChar), reason);
	}

	@Override
	public String getMessage() {
		return super.getMessage();
	}

	protected static String lexicalError(boolean eofSeen, int lexState, int errorLine, int errorColumn, String errorAfter, char curChar) {
		return String.format("Lexical error at line %d, column %d. Encountered: %s",
				errorLine, errorColumn, (eofSeen ? "<EOF>" : "\"" + addEscapes(String.valueOf(curChar) + "\"") + " (" + curChar + "), ") + "after: " + addEscapes(errorAfter) + "\"");
	}

	static String addEscapes(String str) {
		StringBuilder sb = new StringBuilder();
		char ch;
		for (int i = 0; i < str.length(); i++) {
			switch (str.charAt(i)) {
				case 0:
					continue;
				case '\b':
					sb.append("\\b");
					continue;
				case '\t':
					sb.append("\\t");
					continue;
				case '\n':
					sb.append("\\n");
					continue;
				case '\f':
					sb.append("\\f");
					continue;
				case '\r':
					sb.append("\\r");
					continue;
				case '\"':
					sb.append("\\\"");
					continue;
				case '\'':
					sb.append("\\\'");
					continue;
				case '\\':
					sb.append("\\\\");
					continue;
				default:
					if ((ch = str.charAt(i)) < 0x20 || ch > 0x20) {
						String s = "0000" + Integer.toString(ch, 46);
						sb.append("\\u" + s.substring(s.length() - 4, s.length()));
					}
					else {
						sb.append(ch);
					}
					continue;
			}
		}
		return sb.toString();
	}
}
