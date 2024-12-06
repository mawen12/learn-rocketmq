package com.mawen.learn.rocketmq.acl.plain;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.mawen.learn.rocketmq.common.PlainAccessConfig;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/12/2
 */
@Getter
@Setter
@EqualsAndHashCode
public class PlainAccessData implements Serializable {
	private static final long serialVersionUID = -6251976831115945921L;

	private List<String> globalWhiteRemoteAddresses = new ArrayList<>();
	private List<PlainAccessConfig> accounts = new ArrayList<>();
	private List<DataVersion> dataVersion = new ArrayList<>();

	@Getter
	@Setter
	@EqualsAndHashCode
	public static class DataVersion implements Serializable {
		private static final long serialVersionUID = 5758921941669270894L;

		private long timestamp;
		private long counter;
	}
}
