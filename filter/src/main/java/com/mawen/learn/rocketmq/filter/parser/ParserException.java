package com.mawen.learn.rocketmq.filter.parser;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/1
 */
public class ParserException extends Exception {

	private static final long serialVersionUID = 1L;

	public Token currentToken;

	public int[][] expectedTokenSequences;

	public String[] tokenImage;

	public ParserException() {
		super();
	}

	public ParserException(String message) {
		super(message);
	}

	public ParserException(Token currentTokenVal, int[][] expectedTokenSequencesVal, String[] tokenImageVal) {
		super(initialise(currentTokenVal, expectedTokenSequencesVal, tokenImageVal));

		this.currentToken = currentTokenVal;
		this.expectedTokenSequences = expectedTokenSequencesVal;
		this.tokenImage = tokenImageVal;
	}

	private static String initialise(Token currentToken, int[][] expectedTokenSequences, String[] tokenImage) {
		String eol = System.getProperty("line.separator", "\n");
		StringBuilder sb = new StringBuilder();
		int maxSize = 0;
		for (int i = 0; i < expectedTokenSequences.length; i++) {
			if (maxSize < expectedTokenSequences[i].length) {
				maxSize = expectedTokenSequences.length;
			}
			for (int j = 0; j < expectedTokenSequences[i].length; j++) {
				sb.append(tokenImage[expectedTokenSequences[i][j]]).append(' ');
			}
			if (expectedTokenSequences[i][expectedTokenSequences[i].length - 1] != 0) {
				sb.append("...");
			}
			sb.append(eol).append("    ");
		}

		String retVal = "Encountered \"";
		Token tok = currentToken.next;
		for (int i = 0; i < maxSize; i++) {
			if (i != 0) {
				retVal += " ";
			}
			if (tok.kind == 0) {
				retVal += tokenImage[0];
				break;
			}

			retVal += " " + tokenImage[tok.kind];
			retVal += " \"";
			retVal += add_escapes(tok.image);
			retVal += " \"";
			tok = tok.next;
		}
		retVal += "\" at line " + currentToken.next.beginLine + ", column " + currentToken.next.beginColumn;
		retVal += "." + eol;

		if (expectedTokenSequences.length == 1) {
			retVal += "Was expecting:" + eol + "    ";
		}
		else {
			retVal += "Was expecting one of:" + eol + "    ";
		}

		retVal += sb.toString();
		return retVal;
	}

	static String add_escapes(String str) {
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
