package org.apache.ranger.entity;

import javax.persistence.Cacheable;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.xml.bind.annotation.XmlRootElement;

@Entity
@Cacheable
@XmlRootElement
@Table(name = "x_policy")
public class XXPolicyWithAssignedId extends XXPolicyBase {
	private static final long serialVersionUID = 1L;

	/**
	 * id of the XXPolicy
	 *
	 */
	@Id
	@Column(name = "id")
	protected Long id;

	@Override
	public void setId(Long id) {
		this.id = id;
	}

	@Override
	public Long getId() {
		return id;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		XXPolicyWithAssignedId other = (XXPolicyWithAssignedId) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "XXPolicyWithAssignedId [id=" + id + "]";
	}

}
