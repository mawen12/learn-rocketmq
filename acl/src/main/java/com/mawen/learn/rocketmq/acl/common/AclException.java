package com.mawen.learn.rocketmq.acl.common;

import lombok.Getter;
import lombok.Setter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/12/2
 */
@Getter
@Setter
public class AclException extends RuntimeException {

	private static final long serialVersionUID = -8919066803542633792L;

	private String status;
	private int code;

	public AclException(String status, int code) {
		super();
		this.status = status;
		this.code = code;
	}

	public AclException(String status, int code, String message) {
		super(message);
		this.status = status;
		this.code = code;
	}

	public AclException(String message) {
		super(message);
	}

	public AclException(String message, Throwable cause) {
		super(message, cause);
	}

	public AclException(String status, int code, String message, Throwable cause) {
		super(message, cause);
		this.status = status;
		this.code = code;
	}
}
