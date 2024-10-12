package com.mawen.learn.rocketmq.remoting.protocol.body;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
public class AclInfo {

	private String subject;

	private List<PolicyInfo> policies;

	public static AclInfo of(String subject, List<String> resources, List<String> actions, List<String> sourceIps, String decision) {
		AclInfo aclInfo = new AclInfo();
		aclInfo.setSubject(subject);
		aclInfo.setPolicies(Collections.singletonList(PolicyInfo.of(resources, actions, sourceIps, decision)));
		return aclInfo;
	}

	public String getSubject() {
		return subject;
	}

	public void setSubject(String subject) {
		this.subject = subject;
	}

	public List<PolicyInfo> getPolicies() {
		return policies;
	}

	public void setPolicies(List<PolicyInfo> policies) {
		this.policies = policies;
	}

	public static class PolicyInfo {
		private String policyType;

		private List<PolicyEntryInfo> entries;

		public static PolicyInfo of(List<String> resources, List<String> actions, List<String> sourceIps, String decision) {
			PolicyInfo policyInfo = new PolicyInfo();
			List<PolicyEntryInfo> entries = resources.stream()
					.map(resource -> PolicyEntryInfo.of(resource, actions, sourceIps, decision))
					.collect(Collectors.toList());
			policyInfo.setEntries(entries);
			return policyInfo;
		}

		public String getPolicyType() {
			return policyType;
		}

		public void setPolicyType(String policyType) {
			this.policyType = policyType;
		}

		public List<PolicyEntryInfo> getEntries() {
			return entries;
		}

		public void setEntries(List<PolicyEntryInfo> entries) {
			this.entries = entries;
		}
	}

	public static class PolicyEntryInfo {

		private String resource;

		private List<String> actions;

		private List<String> sourceIps;

		private String decision;

		public static PolicyEntryInfo of(String resource, List<String> actions, List<String> sourceIps, String decision) {
			PolicyEntryInfo entry = new PolicyEntryInfo();
			entry.setResource(resource);
			entry.setActions(actions);
			entry.setSourceIps(sourceIps);
			entry.setDecision(decision);
			return entry;
		}

		public String getResource() {
			return resource;
		}

		public void setResource(String resource) {
			this.resource = resource;
		}

		public List<String> getActions() {
			return actions;
		}

		public void setActions(List<String> actions) {
			this.actions = actions;
		}

		public List<String> getSourceIps() {
			return sourceIps;
		}

		public void setSourceIps(List<String> sourceIps) {
			this.sourceIps = sourceIps;
		}

		public String getDecision() {
			return decision;
		}

		public void setDecision(String decision) {
			this.decision = decision;
		}
	}

}
