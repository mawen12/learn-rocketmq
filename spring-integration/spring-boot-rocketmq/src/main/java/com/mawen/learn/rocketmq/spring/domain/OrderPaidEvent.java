package com.mawen.learn.rocketmq.spring.domain;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/5/16
 */
public record OrderPaidEvent(String orderId, BigDecimal paidMoney) implements Serializable {
}
