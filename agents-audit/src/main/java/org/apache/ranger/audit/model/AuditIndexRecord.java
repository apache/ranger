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

package org.apache.ranger.audit.model;

import java.util.Date;

public class AuditIndexRecord {
	String            id;
	String            filePath;
	int               linePosition       = 0;
	SPOOL_FILE_STATUS status             = SPOOL_FILE_STATUS.write_inprogress;
	Date              fileCreateTime;
	Date              writeCompleteTime;
	Date              doneCompleteTime;
	Date              lastSuccessTime;
	Date              lastFailedTime;
	int               failedAttemptCount = 0;
	boolean           lastAttempt        = false;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getFilePath() {
		return filePath;
	}

	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}

	public int getLinePosition() {
		return linePosition;
	}

	public void setLinePosition(int linePosition) {
		this.linePosition = linePosition;
	}

	public SPOOL_FILE_STATUS getStatus() {
		return status;
	}

	public void setStatus(SPOOL_FILE_STATUS status) {
		this.status = status;
	}

	public Date getFileCreateTime() {
		return fileCreateTime;
	}

	public void setFileCreateTime(Date fileCreateTime) {
		this.fileCreateTime = fileCreateTime;
	}

	public Date getWriteCompleteTime() {
		return writeCompleteTime;
	}

	public void setWriteCompleteTime(Date writeCompleteTime) {
		this.writeCompleteTime = writeCompleteTime;
	}

	public Date getDoneCompleteTime() {
		return doneCompleteTime;
	}

	public void setDoneCompleteTime(Date doneCompleteTime) {
		this.doneCompleteTime = doneCompleteTime;
	}

	public Date getLastSuccessTime() {
		return lastSuccessTime;
	}

	public void setLastSuccessTime(Date lastSuccessTime) {
		this.lastSuccessTime = lastSuccessTime;
	}

	public Date getLastFailedTime() {
		return lastFailedTime;
	}

	public void setLastFailedTime(Date lastFailedTime) {
		this.lastFailedTime = lastFailedTime;
	}

	public int getFailedAttemptCount() {
		return failedAttemptCount;
	}

	public void setFailedAttemptCount(int failedAttemptCount) {
		this.failedAttemptCount = failedAttemptCount;
	}

	public boolean getLastAttempt() {
		return lastAttempt;
	}

	public void setLastAttempt(boolean lastAttempt) {
		this.lastAttempt = lastAttempt;
	}

	@Override
	public String toString() {
		return "AuditIndexRecord [id=" + id + ", filePath=" + filePath
			+ ", linePosition=" + linePosition + ", status=" + status
			+ ", fileCreateTime=" + fileCreateTime
			+ ", writeCompleteTime=" + writeCompleteTime
			+ ", doneCompleteTime=" + doneCompleteTime
			+ ", lastSuccessTime=" + lastSuccessTime
			+ ", lastFailedTime=" + lastFailedTime
			+ ", failedAttemptCount=" + failedAttemptCount
			+ ", lastAttempt=" + lastAttempt + "]";
	}
}


