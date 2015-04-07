package org.apache.ranger.view;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.ranger.common.view.VList;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility;
import org.codehaus.jackson.map.annotate.JsonSerialize;

@JsonAutoDetect(getterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE, fieldVisibility = Visibility.ANY)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)

public class VXModuleDefList extends VList {

	private static final long serialVersionUID = 1L;

	List<VXModuleDef> vXModuleDef = new ArrayList<VXModuleDef>();

	public VXModuleDefList() {
		super();
	}

	public VXModuleDefList(List<VXModuleDef> objList) {
		super(objList);
		this.vXModuleDef = objList;
	}

	/**
	 * @return the vXModuleDef
	 */
	public List<VXModuleDef> getvXModuleDef() {
		return vXModuleDef;
	}

	/**
	 * @param vXModuleDef the vXModuleDef to set
	 */
	public void setvXModuleDef(List<VXModuleDef> vXModuleDef) {
		this.vXModuleDef = vXModuleDef;
	}

	@Override
	public int getListSize() {
		if (vXModuleDef != null) {
			return vXModuleDef.size();
		}
		return 0;
	}

	@Override
	public List<VXModuleDef> getList() {
		return vXModuleDef;
	}

}
