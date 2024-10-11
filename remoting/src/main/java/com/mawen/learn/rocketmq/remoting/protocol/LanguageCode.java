package com.mawen.learn.rocketmq.remoting.protocol;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/11
 */
public enum LanguageCode {
	JAVA((byte) 0),
	CPP((byte) 1),
	DOTNET((byte) 2),
	PYTHON((byte) 3),
	DELPHI((byte) 4),
	ERLANG((byte) 5),
	RUBY((byte) 6),
	OTHER((byte) 7),
	HTTP((byte) 8),
	GO((byte) 9),
	PHP((byte) 10),
	OMS((byte) 11),
	RUST((byte) 12)
	;

	private static final Map<String, LanguageCode> MAP = Arrays.stream(LanguageCode.values()).collect(Collectors.toMap(LanguageCode::name, Function.identity()));

	private final byte code;

	LanguageCode(byte code) {
		this.code = code;
	}

	public byte getCode() {
		return code;
	}

	public static LanguageCode valueOf(byte code) {
		for (LanguageCode languageCode : LanguageCode.values()) {
			if (languageCode.getCode() == code) {
				return languageCode;
			}
		}
		return null;
	}

	public static LanguageCode getCode(String language) {
		return MAP.get(language);
	}
}
