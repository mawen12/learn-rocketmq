package com.mawen.learn.rocketmq.common.help;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/9/20
 */
public class FAQUrl {

	public static final String APPLY_TOPIC_URL = "https://rocketmq.apache.org/docs/bestPractice/06FAQ";

	public static final String NAME_SERVER_ADDR_NOT_EXIST_URL = "https://rocketmq.apache.org/docs/bestPractice/06FAQ";

	public static final String GROUP_NAME_DUPLICATE_URL = "https://rocketmq.apache.org/docs/bestPractice/06FAQ";

	public static final String CLIENT_PARAMETER_CHECK_URL = "https://rocketmq.apache.org/docs/bestPractice/06FAQ";

	public static final String SUBSCRIPTION_GROUP_NOT_EXIST = "https://rocketmq.apache.org/docs/bestPractice/06FAQ";

	public static final String CLIENT_SERVICE_NOT_OK = "https://rocketmq.apache.org/docs/bestPractice/06FAQ";

	public static final String NO_TOPIC_ROUTE_INFO = "https://rocketmq.apache.org/docs/bestPractice/06FAQ";

	public static final String LOAD_JSON_EXCEPTION = "https://rocketmq.apache.org/docs/bestPractice/06FAQ";

	public static final String SAME_GROUP_DIFFERENT_TOPIC = "https://rocketmq.apache.org/docs/bestPractice/06FAQ";

	public static final String MQLIST_NOT_EXIST = "https://rocketmq.apache.org/docs/bestPractice/06FAQ";

	public static final String UNEXPECTED_EXCEPTION_URL = "https://rocketmq.apache.org/docs/bestPractice/06FAQ";

	public static final String SEND_MSG_FAILED = "https://rocketmq.apache.org/docs/bestPractice/06FAQ";

	public static final String UNKNOWN_HOST_EXCEPTION = "https://rocketmq.apache.org/docs/bestPractice/06FAQ";

	private static final String TIP_STRING_BEGIN = "\nSee ";
	private static final String TIP_STRING_END = " for further details.";
	private static final String MORE_INFORMATION = "For more information, please visit the url, ";

	public static String suggestTodo(final String url) {
		StringBuilder sb = new StringBuilder(TIP_STRING_BEGIN.length() + url.length() + TIP_STRING_END.length());
		sb.append(TIP_STRING_BEGIN);
		sb.append(url);
		sb.append(TIP_STRING_END);
		return sb.toString();
	}

	public static String attachDefaultURL(final String errorMessage) {
		if (errorMessage != null) {
			int index = errorMessage.indexOf(TIP_STRING_BEGIN);
			if (index == -1) {
				StringBuilder sb = new StringBuilder(errorMessage.length() + UNEXPECTED_EXCEPTION_URL.length() + MORE_INFORMATION.length() + 1);
				sb.append(errorMessage);
				sb.append("\n");
				sb.append(MORE_INFORMATION);
				sb.append(UNEXPECTED_EXCEPTION_URL);
				return sb.toString();
			}
		}
		return errorMessage;
	}
}