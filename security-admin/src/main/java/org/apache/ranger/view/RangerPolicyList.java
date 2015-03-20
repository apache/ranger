package org.apache.ranger.view;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.ranger.common.view.VList;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility;
import org.codehaus.jackson.map.annotate.JsonSerialize;

@JsonAutoDetect(getterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE, fieldVisibility = Visibility.ANY)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class RangerPolicyList extends VList {
	private static final long serialVersionUID = 1L;

	List<RangerPolicy> policies = new ArrayList<RangerPolicy>();

	public RangerPolicyList() {
		super();
	}

	public RangerPolicyList(List<RangerPolicy> objList) {
		super(objList);
		this.policies = objList;
	}

	public List<RangerPolicy> getPolicies() {
		return policies;
	}

	public void setPolicies(List<RangerPolicy> policies) {
		this.policies = policies;
	}

	@Override
	public int getListSize() {
		if (policies != null) {
			return policies.size();
		}
		return 0;
	}

	@Override
	public List<?> getList() {
		return policies;
	}

}
