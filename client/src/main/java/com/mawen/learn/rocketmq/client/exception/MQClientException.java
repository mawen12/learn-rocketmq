package com.mawen.learn.rocketmq.client.exception;

import com.mawen.learn.rocketmq.common.UtilAll;
import com.mawen.learn.rocketmq.common.help.FAQUrl;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingCommand;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/16
 */
public class MQClientException extends Exception{

	private static final long serialVersionUID = 2321990081205124001L;

	private int responseCode;

	private String errorMessage;

	public MQClientException(String errorMessage, Throwable cause) {
		super(FAQUrl.attachDefaultURL(errorMessage), cause);
		this.responseCode = -1;
		this.errorMessage = errorMessage;
	}

	public MQClientException(RemotingCommand response) {
		this(response.getCode(), response.getRemark());
	}

	public MQClientException(int responseCode, String errorMessage) {
		super(FAQUrl.attachDefaultURL("CODE: " + UtilAll.responseCode2String(responseCode) + " DESC: " + errorMessage));
		this.responseCode = responseCode;
		this.errorMessage = errorMessage;
	}

	public MQClientException(int responseCode, String errorMessage, Throwable cause) {
		super(FAQUrl.attachDefaultURL("CODE: " + UtilAll.responseCode2String(responseCode) + " DESC: " + errorMessage), cause);
		this.responseCode = responseCode;
		this.errorMessage = errorMessage;
	}


	public int getResponseCode() {
		return responseCode;
	}

	public void setResponseCode(int responseCode) {
		this.responseCode = responseCode;
	}

	public String getErrorMessage() {
		return errorMessage;
	}

	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}
}
