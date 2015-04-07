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

public class VXUserPermissionList extends VList {

	private static final long serialVersionUID = 1L;

	List<VXUserPermission> vXUserPermission = new ArrayList<VXUserPermission>();

	public VXUserPermissionList() {
		super();
	}

	public VXUserPermissionList(List<VXUserPermission> objList) {
		super(objList);
		this.vXUserPermission = objList;
	}

	/**
	 * @return the vXModuleDef
	 */
	public List<VXUserPermission> getvXModuleDef() {
		return vXUserPermission;
	}

	/**
	 * @param vXModuleDef the vXModuleDef to set
	 */
	public void setvXModuleDef(List<VXUserPermission> vXModuleDef) {
		this.vXUserPermission = vXModuleDef;
	}

	@Override
	public int getListSize() {
		if (vXUserPermission != null) {
			return vXUserPermission.size();
		}
		return 0;
	}

	@Override
	public List<VXUserPermission> getList() {
		return vXUserPermission;
	}
}
