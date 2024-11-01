package com.mawen.learn.rocketmq.filter.parser;

import java.io.Serializable;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/1
 */
public class Token implements Serializable {

	private static final long serialVersionUID = 1L;

	public int kind;

	public int beginLine;

	public int beginColumn;

	public int endLine;

	public int endColumn;

	public String image;

	public Token next;

	public Token specialToken;

	public Token() {
	}

	public Token(int kind) {
		this(kind, null);
	}

	public Token(int kind, String image) {
		this.kind = kind;
		this.image = image;
	}

	public Object getValue() {
		return null;
	}

	@Override
	public String toString() {
		return image;
	}

	public static Token newToken(int ofKind, String image) {
		switch (ofKind) {
			default:
				return new Token(ofKind, image);
		}
	}

	public static Token newToken(int ofKind) {
		return newToken(ofKind, null);
	}
}
