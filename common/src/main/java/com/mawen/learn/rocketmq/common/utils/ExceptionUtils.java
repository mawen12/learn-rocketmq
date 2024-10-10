package com.mawen.learn.rocketmq.common.utils;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/10
 */
public class ExceptionUtils {

	public static Throwable getRealException(Throwable throwable) {
		if (throwable instanceof CompletionException || throwable instanceof ExecutionException) {
			if (throwable.getCause() != null) {
				throwable = throwable.getCause();
			}
		}
		return throwable;
	}

	public static String getErrorDetailMessage(Throwable t) {
		if (t == null) {
			return null;
		}

		StringBuilder sb = new StringBuilder();
		sb.append(t.getMessage()).append(". ").append(t.getClass().getSimpleName());

		if (t.getStackTrace().length > 0) {
			sb.append(". ").append(t.getStackTrace()[0]);
		}
		return sb.toString();
	}
}
