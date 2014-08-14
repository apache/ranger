package com.xasecure.audit.entity;

import com.xasecure.audit.model.EnumRepositoryType;
import java.util.Date;
import javax.annotation.Generated;
import javax.persistence.metamodel.SingularAttribute;
import javax.persistence.metamodel.StaticMetamodel;

@Generated(value="Dali", date="2014-02-04T07:25:42.940-0800")
@StaticMetamodel(XXBaseAuditEvent.class)
public class XXBaseAuditEvent_ {
	public static volatile SingularAttribute<XXBaseAuditEvent, Long> auditId;
	public static volatile SingularAttribute<XXBaseAuditEvent, String> agentId;
	public static volatile SingularAttribute<XXBaseAuditEvent, String> user;
	public static volatile SingularAttribute<XXBaseAuditEvent, Date> timeStamp;
	public static volatile SingularAttribute<XXBaseAuditEvent, Long> policyId;
	public static volatile SingularAttribute<XXBaseAuditEvent, String> accessType;
	public static volatile SingularAttribute<XXBaseAuditEvent, Short> accessResult;
	public static volatile SingularAttribute<XXBaseAuditEvent, String> resultReason;
	public static volatile SingularAttribute<XXBaseAuditEvent, String> aclEnforcer;
	public static volatile SingularAttribute<XXBaseAuditEvent, EnumRepositoryType> repositoryType;
	public static volatile SingularAttribute<XXBaseAuditEvent, String> repositoryName;
	public static volatile SingularAttribute<XXBaseAuditEvent, String> sessionId;
	public static volatile SingularAttribute<XXBaseAuditEvent, String> clientType;
	public static volatile SingularAttribute<XXBaseAuditEvent, String> clientIP;
	public static volatile SingularAttribute<XXBaseAuditEvent, String> action;
}
