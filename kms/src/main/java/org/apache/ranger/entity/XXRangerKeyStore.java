package org.apache.ranger.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;
import javax.xml.bind.annotation.XmlRootElement;

@Entity
@Table(name="ranger_keystore")
@XmlRootElement
public class XXRangerKeyStore extends XXDBBase implements java.io.Serializable {
	private static final long serialVersionUID = 1L;
	
	@Id
	@SequenceGenerator(name="kmskeys",sequenceName="kmskeys",allocationSize=1)
	@GeneratedValue(strategy=GenerationType.AUTO,generator="kmskeys")
	@Column(name="ID")
	protected Long id;
	@Override
	public void setId(Long id) {
		this.id=id;
	}

	@Override
	public Long getId() {
		return id;
	}
	
	@Column(name="kms_alias"  , length=255 )
	protected String alias;
	
	public String getAlias() {
		return alias;
	}

	public void setAlias(String alias) {
		this.alias = alias;
	}
	
	@Column(name="kms_createdDate"  , length=255 )
	protected Long createdDate;
	
	public Long getCreatedDate() {
		return createdDate;
	}

	public void setCreatedDate(Long createdDate) {
		this.createdDate = createdDate;
	}

	@Lob
	@Column(name="kms_encoded")
	protected String encoded;

	public String getEncoded() {
		return encoded;
	}

	public void setEncoded(String encoded) {
		this.encoded = encoded;
	}
	
	@Column(name="kms_cipher" , length=255)
	protected String cipher;
	
	public String getCipher() {
		return cipher;
	}

	public void setCipher(String cipher) {
		this.cipher = cipher;
	}

	@Column(name="kms_bitLength")
	protected int bitLength;
	
	public int getBitLength() {
		return bitLength;
	}

	public void setBitLength(int bitLength) {
		this.bitLength = bitLength;
	}
	
	@Column(name="kms_description")
	protected String description;
	
	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}
	
	@Column(name="kms_version")
	protected int version;
	
	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}
	
	@Column(name="kms_attributes")
	protected String attributes;
	public String getAttributes() {
		return attributes;
	}

	public void setAttributes(String attributes) {
		this.attributes = attributes;
	}	
}
