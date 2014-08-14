package com.xasecure.audit.entity;

import javax.annotation.Generated;
import javax.persistence.metamodel.SingularAttribute;
import javax.persistence.metamodel.StaticMetamodel;

@Generated(value="Dali", date="2014-02-02T14:05:13.483-0800")
@StaticMetamodel(XXHBaseAuditEvent.class)
public class XXHBaseAuditEvent_ extends XXBaseAuditEvent_ {
	public static volatile SingularAttribute<XXHBaseAuditEvent, String> resourcePath;
	public static volatile SingularAttribute<XXHBaseAuditEvent, String> resourceType;
	public static volatile SingularAttribute<XXHBaseAuditEvent, String> requestData;
}
