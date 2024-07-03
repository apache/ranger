/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

 package org.apache.ranger.entity;

/**
 * Logging table for all DB create and update queries
 *
 */

import org.apache.ranger.common.DateUtil;
import org.apache.ranger.common.RangerConstants;

import javax.persistence.*;
import java.util.Date;


@Entity
@Table(name="x_trx_log_v2")
public class XXTrxLogV2 implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    @Id
    @SequenceGenerator(name="X_TRX_LOG_V2_SEQ", sequenceName="X_TRX_LOG_V2_SEQ", allocationSize=1)
    @GeneratedValue(strategy=GenerationType.AUTO, generator="X_TRX_LOG_V2_SEQ")
    @Column(name="ID")
    protected Long id;

    @Temporal(TemporalType.TIMESTAMP)
    @Column(name="CREATE_TIME")
    protected Date createTime = DateUtil.getUTCDate();

    @Column(name="ADDED_BY_ID")
    protected Long addedByUserId;

    @Column(name="CLASS_TYPE", nullable=false )
    protected int  objectClassType = RangerConstants.CLASS_TYPE_NONE;

    @Column(name="OBJECT_ID")
    protected Long objectId;

    @Column(name="OBJECT_NAME")
    protected String objectName;

    @Column(name="PARENT_OBJECT_CLASS_TYPE", nullable=false )
    protected int parentObjectClassType;

    @Column(name="PARENT_OBJECT_ID")
    protected Long parentObjectId;

    @Column(name="PARENT_OBJECT_NAME", length=1024)
    protected String parentObjectName;

    @Column(name="ACTION", length=255)
    protected String action;

    @Column(name="CHANGE_INFO")
    protected String changeInfo;

    @Column(name="TRX_ID", length=1024)
    protected String transactionId;

    @Column(name="REQ_ID")
    protected String requestId;

    @Column(name="SESS_ID", length=512)
    protected String sessionId;

    @Column(name="SESS_TYPE")
    protected String sessionType;

    /**
     * Default constructor. This will set all the attributes to default value.
     */
    public XXTrxLogV2( ) {
    }

    public XXTrxLogV2(int objectClassType, Long objectId, String objectName, String action) {
        this.objectClassType = objectClassType;
        this.objectId        = objectId;
        this.objectName      = objectName;
        this.action          = action;
    }

    public XXTrxLogV2(int objectClassType, Long objectId, String objectName, int parentObjectClassType, Long parentObjectId, String parentObjectName, String action) {
        this.objectClassType       = objectClassType;
        this.objectId              = objectId;
        this.objectName            = objectName;
        this.parentObjectClassType = parentObjectClassType;
        this.parentObjectId        = parentObjectId;
        this.parentObjectName      = parentObjectName;
        this.action                = action;
    }

    public XXTrxLogV2(int objectClassType, Long objectId, String objectName, int parentObjectClassType, Long parentObjectId, String parentObjectName, String action, String changeInfo) {
        this.objectClassType       = objectClassType;
        this.objectId              = objectId;
        this.objectName            = objectName;
        this.parentObjectClassType = parentObjectClassType;
        this.parentObjectId        = parentObjectId;
        this.parentObjectName      = parentObjectName;
        this.action                = action;
        this.changeInfo            = changeInfo;
    }

    public void setId(Long id) {
        this.id=id;
    }

    public Long getId() {
        return id;
    }

    public void setCreateTime( Date createTime ) {
        this.createTime = createTime;
    }

    public Date getCreateTime( ) {
        return this.createTime;
    }

    public void setAddedByUserId( Long addedByUserId ) {
        this.addedByUserId = addedByUserId;
    }

    public Long getAddedByUserId( ) {
        return this.addedByUserId;
    }

    public void setObjectClassType( int objectClassType ) {
        this.objectClassType = objectClassType;
    }

    public int getObjectClassType( ) {
        return this.objectClassType;
    }

    public void setObjectId( Long objectId ) {
        this.objectId = objectId;
    }

    public Long getObjectId( ) {
        return this.objectId;
    }

    public void setObjectName( String objectName ) {
        this.objectName = objectName;
    }

    public String getObjectName( ) {
        return this.objectName;
    }

    public void setParentObjectClassType( int parentObjectClassType ) {
        this.parentObjectClassType = parentObjectClassType;
    }

    public int getParentObjectClassType( ) {
        return this.parentObjectClassType;
    }

    public void setParentObjectId( Long parentObjectId ) {
        this.parentObjectId = parentObjectId;
    }

    public Long getParentObjectId( ) {
        return this.parentObjectId;
    }

    public void setParentObjectName( String parentObjectName ) {
        this.parentObjectName = parentObjectName;
    }

    public String getParentObjectName( ) {
        return this.parentObjectName;
    }

    public void setAction( String action ) {
        this.action = action;
    }

    public String getAction( ) {
        return this.action;
    }

    public void setChangeInfo( String changeInfo ) {
        this.changeInfo = changeInfo;
    }

    public String getChangeInfo( ) {
        return this.changeInfo;
    }

    public void setTransactionId( String transactionId ) {
        this.transactionId = transactionId;
    }

    public String getTransactionId( ) {
        return this.transactionId;
    }

    public void setRequestId( String requestId ) {
        this.requestId = requestId;
    }

    public String getRequestId( ) {
        return this.requestId;
    }

    public void setSessionId( String sessionId ) {
        this.sessionId = sessionId;
    }

    public String getSessionId( ) {
        return this.sessionId;
    }

    public void setSessionType( String sessionType ) {
        this.sessionType = sessionType;
    }

    public String getSessionType( ) {
        return this.sessionType;
    }

    @Override
    public String toString( ) {
        String str = "XXTrxLogV2={";
        str += super.toString();
        str += "objectClassType={" + objectClassType + "} ";
        str += "objectId={" + objectId + "} ";
        str += "parentObjectId={" + parentObjectId + "} ";
        str += "parentObjectClassType={" + parentObjectClassType + "} ";
        str += "parentObjectName={" + parentObjectName + "} ";
        str += "objectName={" + objectName + "} ";
        str += "changeInfo={" + changeInfo + "} ";
        str += "transactionId={" + transactionId + "} ";
        str += "action={" + action + "} ";
        str += "requestId={" + requestId + "} ";
        str += "}";
        return str;
    }
}
