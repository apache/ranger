package org.apache.ranger.view;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.ranger.common.view.VList;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility;
import org.codehaus.jackson.map.annotate.JsonSerialize;

@JsonAutoDetect(getterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE, fieldVisibility = Visibility.ANY)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class RangerServiceDefList extends VList {
	private static final long serialVersionUID = 1L;

	List<RangerServiceDef> serviceDefs = new ArrayList<RangerServiceDef>();

	public RangerServiceDefList() {
		super();
	}

	public RangerServiceDefList(List<RangerServiceDef> objList) {
		super(objList);
		this.serviceDefs = objList;
	}

	public List<RangerServiceDef> getServiceDefs() {
		return serviceDefs;
	}

	public void setServiceDefs(List<RangerServiceDef> serviceDefs) {
		this.serviceDefs = serviceDefs;
	}

	@Override
	public int getListSize() {
		if (serviceDefs != null) {
			return serviceDefs.size();
		}
		return 0;
	}

	@Override
	public List<?> getList() {
		return serviceDefs;
	}

}
