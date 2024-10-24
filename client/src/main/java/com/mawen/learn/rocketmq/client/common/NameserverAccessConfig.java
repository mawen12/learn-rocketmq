package com.mawen.learn.rocketmq.client.common;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/24
 */
@AllArgsConstructor
@Getter
public class NameserverAccessConfig {
	private String namesrvAddr;
	private String namesrvDomain;
	private String namesrvDomainSubgroup;
}
