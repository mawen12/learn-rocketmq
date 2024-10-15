package com.mawen.learn.rocketmq.common;

import com.mawen.learn.rocketmq.common.help.FAQUrl;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/9/20
 */
public class AbortProcessException extends RuntimeException {

	private static final long serialVersionUID = 5739592731939329575L;

	private final int responseCode;
	private final String errorMessage;

	public AbortProcessException(String errorMessage, Throwable cause) {
		super(FAQUrl.attachDefaultURL(errorMessage), cause);
		this.responseCode = -1;
		this.errorMessage = errorMessage;
	}

	public AbortProcessException(int responseCode, String errorMessage) {
		super(FAQUrl.attachDefaultURL("CODE: " + UtilAll.responseCode2String(responseCode) + " DESC: " + errorMessage));
		this.responseCode = responseCode;
		this.errorMessage = errorMessage;
	}

	public int getResponseCode() {
		return responseCode;
	}

	public String getErrorMessage() {
		return errorMessage;
	}
}
