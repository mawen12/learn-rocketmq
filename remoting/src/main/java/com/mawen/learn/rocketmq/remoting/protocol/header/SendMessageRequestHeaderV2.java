package com.mawen.learn.rocketmq.remoting.protocol.header;

import java.util.Map;

import com.google.common.base.MoreObjects;
import com.mawen.learn.rocketmq.common.action.Action;
import com.mawen.learn.rocketmq.common.action.RocketMQAction;
import com.mawen.learn.rocketmq.common.resource.ResourceType;
import com.mawen.learn.rocketmq.common.resource.RocketMQResource;
import com.mawen.learn.rocketmq.remoting.CommandCustomHeader;
import com.mawen.learn.rocketmq.remoting.annotation.CFNotNull;
import com.mawen.learn.rocketmq.remoting.annotation.CFNullable;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;
import com.mawen.learn.rocketmq.remoting.protocol.FastCodesHeader;
import com.mawen.learn.rocketmq.remoting.protocol.RequestCode;
import com.mawen.learn.rocketmq.remoting.rpc.TopicQueueRequestHeader;
import io.netty.buffer.ByteBuf;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/11
 */
@RocketMQAction(value = RequestCode.SEND_MESSAGE_V2, action = Action.PUB)
public class SendMessageRequestHeaderV2 extends TopicQueueRequestHeader implements CommandCustomHeader, FastCodesHeader {

	@CFNotNull
	private String a;

	@CFNotNull
	@RocketMQResource(ResourceType.TOPIC)
	private String b;

	@CFNotNull
	private String c;

	@CFNotNull
	private Integer d;

	@CFNotNull
	private Integer e;

	@CFNotNull
	private Integer f;

	@CFNotNull
	private Long g;

	@CFNullable
	private Integer h;

	@CFNotNull
	private String i;

	@CFNotNull
	private Integer j;

	@CFNotNull
	private Boolean k;

	private Integer l;

	@CFNullable
	private Boolean m;

	@CFNullable
	private String n;

	public static SendMessageRequestHeader createSendMessageRequestHeaderV1(final SendMessageRequestHeaderV2 v2) {
		SendMessageRequestHeader v1 = new SendMessageRequestHeader();
		v1.setProducerGroup(v2.a);
		v1.setTopic(v2.b);
		v1.setDefaultTopic(v2.c);
		v1.setDefaultTopicQueueNums(v2.d);
		v1.setQueueId(v2.e);
		v1.setSysFlag(v2.f);
		v1.setBornTimestamp(v2.g);
		v1.setFlag(v2.h);
		v1.setProperties(v2.i);
		v1.setReconsumeTimes(v2.j);
		v1.setUnitMode(v2.k);
		v1.setMaxReconsumeTimes(v2.l);
		v1.setBatch(v2.m);
		v1.setBrokerName(v2.n);
		return v1;
	}

	public static SendMessageRequestHeaderV2 createSendMessageRequestHeaderV2(final SendMessageRequestHeader v1) {
		SendMessageRequestHeaderV2 v2 = new SendMessageRequestHeaderV2();
		v2.a = v1.getProducerGroup();
		v2.b = v1.getTopic();
		v2.c = v1.getDefaultTopic();
		v2.d = v1.getDefaultTopicQueueNums();
		v2.e = v1.getQueueId();
		v2.f = v1.getSysFlag();
		v2.g = v1.getBornTimestamp();
		v2.h = v1.getFlag();
		v2.i = v1.getProperties();
		v2.j = v1.getReconsumeTimes();
		v2.k = v1.isUnitMode();
		v2.l = v1.getMaxReconsumeTimes();
		v2.m = v1.isBatch();
		v2.n = v1.getBrokerName();
		return v2;
	}

	@Override
	public void encode(ByteBuf out) {
		writeIfNotNull(out, "a", a);
		writeIfNotNull(out, "b", b);
		writeIfNotNull(out, "c", c);
		writeIfNotNull(out, "d", d);
		writeIfNotNull(out, "e", e);
		writeIfNotNull(out, "f", f);
		writeIfNotNull(out, "g", g);
		writeIfNotNull(out, "h", h);
		writeIfNotNull(out, "i", i);
		writeIfNotNull(out, "j", j);
		writeIfNotNull(out, "k", k);
		writeIfNotNull(out, "l", l);
		writeIfNotNull(out, "m", m);
		writeIfNotNull(out, "n", n);
	}

	@Override
	public void decode(Map<String, String> fields) throws RemotingCommandException {
		this.a = getAndCheckNotNull(fields, "a");
		this.b = getAndCheckNotNull(fields, "b");
		this.c = getAndCheckNotNull(fields, "c");
		this.d = Integer.valueOf(getAndCheckNotNull(fields, "d"));
		this.e = Integer.valueOf(getAndCheckNotNull(fields, "e"));
		this.f = Integer.valueOf(getAndCheckNotNull(fields, "f"));
		this.g = Long.valueOf(getAndCheckNotNull(fields, "g"));
		this.h = Integer.valueOf(getAndCheckNotNull(fields, "h"));

		String str = fields.get("i");
		if (str != null) {
			this.i = str;
		}

		str = fields.get("j");
		if (str != null) {
			this.j = Integer.valueOf(str);
		}

		str = fields.get("k");
		if (str != null) {
			this.k = Boolean.valueOf(str);
		}

		str = fields.get("l");
		if (str != null) {
			this.l = Integer.valueOf(str);
		}

		str = fields.get("m");
		if (str != null) {
			this.m = Boolean.valueOf(str);
		}

		str = fields.get("n");
		if (str != null) {
			this.n = str;
		}
	}

	@Override
	public Integer getQueueId() {
		return e;
	}

	@Override
	public void setQueueId(Integer queueId) {
		this.e = queueId;
	}

	@Override
	public String getTopic() {
		return b;
	}

	@Override
	public void setTopic(String topic) {
		this.b = b;
	}

	@Override
	public void checkFields() throws RemotingCommandException {

	}

	public String getA() {
		return a;
	}

	public void setA(String a) {
		this.a = a;
	}

	public String getB() {
		return b;
	}

	public void setB(String b) {
		this.b = b;
	}

	public String getC() {
		return c;
	}

	public void setC(String c) {
		this.c = c;
	}

	public Integer getD() {
		return d;
	}

	public void setD(Integer d) {
		this.d = d;
	}

	public Integer getE() {
		return e;
	}

	public void setE(Integer e) {
		this.e = e;
	}

	public Integer getF() {
		return f;
	}

	public void setF(Integer f) {
		this.f = f;
	}

	public Long getG() {
		return g;
	}

	public void setG(Long g) {
		this.g = g;
	}

	public String getI() {
		return i;
	}

	public void setI(String i) {
		this.i = i;
	}

	public Integer getJ() {
		return j;
	}

	public void setJ(Integer j) {
		this.j = j;
	}

	public Boolean getK() {
		return k;
	}

	public void setK(Boolean k) {
		this.k = k;
	}

	public Integer getL() {
		return l;
	}

	public void setL(Integer l) {
		this.l = l;
	}

	public Boolean getM() {
		return m;
	}

	public void setM(Boolean m) {
		this.m = m;
	}

	public String getN() {
		return n;
	}

	public void setN(String n) {
		this.n = n;
	}

	public Integer getH() {
		return h;
	}

	public void setH(Integer h) {
		this.h = h;
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this)
				.add("a", a)
				.add("b", b)
				.add("c", c)
				.add("d", d)
				.add("e", e)
				.add("f", f)
				.add("g", g)
				.add("h", h)
				.add("i", i)
				.add("j", j)
				.add("k", k)
				.add("l", l)
				.add("m", m)
				.add("n", n)
				.toString();
	}
}
