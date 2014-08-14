package com.xasecure.view;

import java.util.*;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import com.xasecure.common.view.VList;

@JsonAutoDetect(getterVisibility=Visibility.NONE, setterVisibility=Visibility.NONE, fieldVisibility=Visibility.ANY)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL )
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class VXAuditRecordList extends VList {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	List<VXAuditRecord> vXAuditRecords = new ArrayList<VXAuditRecord>();

	public List<VXAuditRecord> getvAudits() {
		return vXAuditRecords;
	}

	public void setvAudits(List<VXAuditRecord> vXAuditRecords) {
		this.vXAuditRecords = vXAuditRecords;
	}

	public VXAuditRecordList() {
		super();
	}

	@Override
	public int getListSize() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public List<?> getList() {
		// TODO Auto-generated method stub
		return null;
	}
}
