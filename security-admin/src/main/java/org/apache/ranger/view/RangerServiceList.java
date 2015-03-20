package org.apache.ranger.view;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.ranger.common.view.VList;
import org.apache.ranger.plugin.model.RangerService;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility;
import org.codehaus.jackson.map.annotate.JsonSerialize;

@JsonAutoDetect(getterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE, fieldVisibility = Visibility.ANY)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class RangerServiceList extends VList {
	private static final long serialVersionUID = 1L;

	List<RangerService> services = new ArrayList<RangerService>();

	public RangerServiceList() {
		super();
	}

	public RangerServiceList(List<RangerService> objList) {
		super(objList);
		this.services = objList;
	}

	public List<RangerService> getServices() {
		return services;
	}

	public void setServices(List<RangerService> services) {
		this.services = services;
	}

	@Override
	public int getListSize() {
		if (services != null) {
			return services.size();
		}
		return 0;
	}

	@Override
	public List<?> getList() {
		return services;
	}

}