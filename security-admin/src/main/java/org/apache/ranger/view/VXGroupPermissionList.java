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

public class VXGroupPermissionList extends VList {

	private static final long serialVersionUID = 1L;

	List<VXGroupPermission> vXGroupPermission = new ArrayList<VXGroupPermission>();

	public VXGroupPermissionList() {
		super();
	}

	public VXGroupPermissionList(List<VXGroupPermission> objList) {
		super(objList);
		this.vXGroupPermission = objList;
	}

	/**
	 * @return the vXGroupPermission
	 */
	public List<VXGroupPermission> getvXGroupPermission() {
		return vXGroupPermission;
	}

	/**
	 * @param vXGroupPermission the vXGroupPermission to set
	 */
	public void setvXGroupPermission(List<VXGroupPermission> vXGroupPermission) {
		this.vXGroupPermission = vXGroupPermission;
	}

	@Override
	public int getListSize() {
		if (vXGroupPermission != null) {
			return vXGroupPermission.size();
		}
		return 0;
	}

	@Override
	public List<VXGroupPermission> getList() {
		return vXGroupPermission;
	}
}
