package com.mawen.learn.rocketmq.client.exception;

import com.mawen.learn.rocketmq.common.UtilAll;
import com.mawen.learn.rocketmq.common.help.FAQUrl;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/16
 */
public class MQBrokerException extends Exception {

	private static final long serialVersionUID = 7674849096497926941L;

	private final int responseCode;

	private final String errorMessage;

	private final String brokerAddr;

	public MQBrokerException() {
		this.responseCode = 0;
		this.errorMessage = null;
		this.brokerAddr = null;
	}

	public MQBrokerException(int responseCode, String errorMessage) {
		super(FAQUrl.attachDefaultURL("CODE: " + UtilAll.responseCode2String(responseCode) + " DESC: " + errorMessage));
		this.responseCode = responseCode;
		this.errorMessage = errorMessage;
		this.brokerAddr = null;
	}

	public MQBrokerException(int responseCode, String errorMessage, String brokerAddr) {
		super(FAQUrl.attachDefaultURL("CODE: " + UtilAll.responseCode2String(responseCode) + " DESC: " + errorMessage + (brokerAddr != null ? " BROKER: " + brokerAddr : "")));
		this.responseCode = responseCode;
		this.errorMessage = errorMessage;
		this.brokerAddr = brokerAddr;
	}

	public int getResponseCode() {
		return responseCode;
	}

	public String getErrorMessage() {
		return errorMessage;
	}

	public String getBrokerAddr() {
		return brokerAddr;
	}
}
