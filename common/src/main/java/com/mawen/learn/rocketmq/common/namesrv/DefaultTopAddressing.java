package com.mawen.learn.rocketmq.common.namesrv;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import com.google.common.base.Strings;
import com.mawen.learn.rocketmq.common.MixAll;
import com.mawen.learn.rocketmq.common.UtilAll;
import com.mawen.learn.rocketmq.common.constant.LoggerName;
import com.mawen.learn.rocketmq.common.help.FAQUrl;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/6
 */
public class DefaultTopAddressing implements TopAddressing {

	private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);
	private static final Log log = LogFactory.getLog(DefaultTopAddressing.class);

	private String nsAddr;
	private String wsAddr;
	private String unitName;
	private Map<String, String> para;
	private List<TopAddressing> topAddressingList;

	public DefaultTopAddressing(String wsAddr) {
		this(wsAddr, null);
	}

	public DefaultTopAddressing(String wsAddr, String unitName) {
		this.wsAddr = wsAddr;
		this.unitName = unitName;
		this.topAddressingList = loadCustomTopAddressing();
	}

	public DefaultTopAddressing(String unitName, Map<String, String> para, String wsAddr) {
		this.wsAddr = wsAddr;
		this.unitName = unitName;
		this.para = para;
		this.topAddressingList = loadCustomTopAddressing();
	}

	public String getNsAddr() {
		return nsAddr;
	}

	public void setNsAddr(String nsAddr) {
		this.nsAddr = nsAddr;
	}

	@Override
	public String fetchNSAddr() {
		if (!topAddressingList.isEmpty()) {
			for (TopAddressing topAddressing : topAddressingList) {
				String nsAddress = topAddressing.fetchNSAddr();
				if (!Strings.isNullOrEmpty(nsAddress)) {
					return nsAddress;
				}
			}
		}
		return fetchNSAddr(true, 3000);
	}

	@Override
	public void registerChangeCallback(NameServerUpdateCallback changeCallback) {
		if (!topAddressingList.isEmpty()) {
			for (TopAddressing topAddressing : topAddressingList) {
				topAddressing.registerChangeCallback(changeCallback);
			}
		}
	}

	public final String fetchNSAddr(boolean verbose, long timeoutMillis) {
		StringBuilder url = new StringBuilder(this.wsAddr);
		try {
			if (para != null && para.size() > 0) {
				if (!UtilAll.isBlank(this.unitName)) {
					url.append("-").append(this.unitName).append("?nofix=1&");
				}
				else {
					url.append("?");
				}

				for (Map.Entry<String, String> entry : this.para.entrySet()) {
					url.append(entry.getKey()).append("=").append(entry.getValue()).append("&");
				}
				url = new StringBuilder(url.substring(0, url.length() - 1));
			}
			else {
				if (!UtilAll.isBlank(this.unitName)) {
					url.append("-").append(this.unitName).append("?nofix=1&");
				}
			}

			HttpTinyClient.HttpResult result = HttpTinyClient.httpGet(url.toString(), null, null, "UTF-8", timeoutMillis);
			if (result.code == 200) {
				String responseStr = result.content;
				if (responseStr != null) {
					return clearNewLine(responseStr);
				}
				else {
					log.error("fetch nameserver address is null");
				}
			}
			else {
				log.error("fetch nameserver address failed. statusCode = " + result.code);
			}
		}
		catch (IOException e) {
			if (verbose) {
				log.error("fetch name server address exception", e);
			}
		}

		if (verbose) {
			String errorMsg = "connect to " + url + " failed, maybe the domain name " + MixAll.getWSAddr() + " not bind in /etc/hosts";
			errorMsg += FAQUrl.suggestTodo(FAQUrl.NAME_SERVER_ADDR_NOT_EXIST_URL);

			log.warn(errorMsg);
		}
		return null;
	}

	private static String clearNewLine(final String str) {
		String newString = str.trim();
		int index = newString.indexOf("\r");
		if (index != -1) {
			return newString.substring(0, index);
		}

		index = newString.indexOf("\n");
		if (index != -1) {
			return newString.substring(0, index);
		}

		return newString;
	}

	private List<TopAddressing> loadCustomTopAddressing() {
		ServiceLoader<TopAddressing> serviceLoader = ServiceLoader.load(TopAddressing.class);
		Iterator<TopAddressing> iterator = serviceLoader.iterator();
		List<TopAddressing> topAddressingList = new ArrayList<>();
		if (iterator.hasNext()) {
			topAddressingList.add(iterator.next());
		}
		return topAddressingList;
	}
}
