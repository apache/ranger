package com.xasecure.audit.entity;

import javax.annotation.Generated;
import javax.persistence.metamodel.SingularAttribute;
import javax.persistence.metamodel.StaticMetamodel;

@Generated(value="Dali", date="2014-02-02T14:05:13.494-0800")
@StaticMetamodel(XXHiveAuditEvent.class)
public class XXHiveAuditEvent_ extends XXBaseAuditEvent_ {
	public static volatile SingularAttribute<XXHiveAuditEvent, String> resourcePath;
	public static volatile SingularAttribute<XXHiveAuditEvent, String> resourceType;
	public static volatile SingularAttribute<XXHiveAuditEvent, String> requestData;
}
