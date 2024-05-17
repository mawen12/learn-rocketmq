package com.mawen.learn.rocketmq.spring.consumer;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionState;
import org.apache.rocketmq.spring.support.RocketMQHeaders;

import org.springframework.messaging.Message;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/5/17
 */
@Slf4j
@RocketMQTransactionListener
public class TransactionListenerImpl implements RocketMQLocalTransactionListener {

	private AtomicInteger transactionIndex =  new AtomicInteger(0);

	private ConcurrentHashMap<String, Integer> localTrans = new ConcurrentHashMap<>();

	@Override
	public RocketMQLocalTransactionState executeLocalTransaction(Message message, Object o) {
		String transId = (String) message.getHeaders().get(RocketMQHeaders.TRANSACTION_ID);
		log.info("#### executeLocalTransaction is executed, msgTransactionId = {}", transId);

		int value = transactionIndex.incrementAndGet();
		int status = value % 3;
		localTrans.put(transId, status);
		if (status == 0) {
			log.info("# Commit # Simulating msg {} related local transaction exec succeeded! ###", message.getPayload());
			return RocketMQLocalTransactionState.COMMIT;
		}

		if (status == 1) {
			log.info("# Rollback # Simulating {} related local transaction exec failed!", message.getPayload());
			return RocketMQLocalTransactionState.ROLLBACK;
		}

		log.info("# UNKNOWN # Simulating {} related local transaction exec UNKNOWN!", message.getPayload());
		return RocketMQLocalTransactionState.UNKNOWN;
	}

	@Override
	public RocketMQLocalTransactionState checkLocalTransaction(Message message) {
		String transId = (String) message.getHeaders().get(RocketMQHeaders.TRANSACTION_ID);
		RocketMQLocalTransactionState retState = RocketMQLocalTransactionState.COMMIT;
		Integer status = localTrans.get(transId);
		if (status != null) {
			switch (status) {
				case 0:
					retState = RocketMQLocalTransactionState.COMMIT;
					break;
				case 1:
					retState = RocketMQLocalTransactionState.ROLLBACK;
					break;
				case 2:
					retState = RocketMQLocalTransactionState.UNKNOWN;
					break;
			}
		}

		log.info("------ !!! checkLocalTransaction is executed once, msgTransactionId = {}, transactionState = {}, status = {}------",
				transId, retState, status);
		return retState;
	}
}
