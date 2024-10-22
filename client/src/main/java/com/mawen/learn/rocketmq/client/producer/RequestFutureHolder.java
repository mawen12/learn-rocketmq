package com.mawen.learn.rocketmq.client.producer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;

import com.mawen.learn.rocketmq.client.common.ClientErrorCode;
import com.mawen.learn.rocketmq.client.exception.RequestTimeoutException;
import com.mawen.learn.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import lombok.Getter;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/22
 */
public class RequestFutureHolder {
	private static final Logger log = LoggerFactory.getLogger(RequestFutureHolder.class);

	private static final RequestFutureHolder INSTANCE = new RequestFutureHolder();

	@Getter
	private ConcurrentHashMap<String, RequestResponseFuture> requestFutureTable = new ConcurrentHashMap<>();

	private final Set<DefaultMQProducerImpl> producerSet = new HashSet<>();

	private ScheduledExecutorService scheduledExecutorService;

	private void scanExpiredRequest() {
		final List<RequestResponseFuture> rfList = new ArrayList<>();
		Iterator<Map.Entry<String, RequestResponseFuture>> iterator = requestFutureTable.entrySet().iterator();
		while (iterator.hasNext()) {
			Map.Entry<String, RequestResponseFuture> next = iterator.next();
			RequestResponseFuture rep = next.getValue();

			if (rep.isTimeout()) {
				iterator.remove();
				rfList.add(rep);
				log.warn("remove timeout request, CorrelationId={}", rep.getCorrelationId());
			}
		}

		for (RequestResponseFuture rf : rfList) {
			try {
				Throwable cause = new RequestTimeoutException(ClientErrorCode.REQUEST_TIMEOUT_EXCEPTION, "request timeout, on reply message.");
				rf.setCause(cause);
				rf.executeRequestCallback();
			}
			catch (Throwable e) {
				log.warn("scanResponseTable, operationComplete Exception", e);
			}
		}
	}

	private RequestFutureHolder() {
	}
}
