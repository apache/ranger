/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ranger.common;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestRangerCommonEnums {
    @Test
    public void testGetLabelFor_VisibilityStatus() {
        assertEquals("Hidden", RangerCommonEnums.getLabelFor_VisibilityStatus(0));
        assertEquals("Visible", RangerCommonEnums.getLabelFor_VisibilityStatus(1));
        assertNull(RangerCommonEnums.getLabelFor_VisibilityStatus(-1));
        assertNull(RangerCommonEnums.getLabelFor_VisibilityStatus(2));
    }

    @Test
    public void testGetLabelFor_ActiveStatus() {
        assertEquals("Disabled", RangerCommonEnums.getLabelFor_ActiveStatus(0));
        assertEquals("Enabled", RangerCommonEnums.getLabelFor_ActiveStatus(1));
        assertEquals("Deleted", RangerCommonEnums.getLabelFor_ActiveStatus(2));
        assertNull(RangerCommonEnums.getLabelFor_ActiveStatus(-1));
        assertNull(RangerCommonEnums.getLabelFor_ActiveStatus(3));
    }

    @Test
    public void testGetLabelFor_ActivationStatus() {
        assertEquals("Disabled", RangerCommonEnums.getLabelFor_ActivationStatus(0));
        assertEquals("Active", RangerCommonEnums.getLabelFor_ActivationStatus(1));
        assertEquals("Pending Approval", RangerCommonEnums.getLabelFor_ActivationStatus(2));
        assertEquals("Pending Activation", RangerCommonEnums.getLabelFor_ActivationStatus(3));
        assertEquals("Rejected", RangerCommonEnums.getLabelFor_ActivationStatus(4));
        assertEquals("Deactivated", RangerCommonEnums.getLabelFor_ActivationStatus(5));
        assertEquals("Registration Pending", RangerCommonEnums.getLabelFor_ActivationStatus(6));
        assertEquals("No login privilege", RangerCommonEnums.getLabelFor_ActivationStatus(7));

        // Edge/Invalid values
        assertNull(RangerCommonEnums.getLabelFor_ActivationStatus(-1));
        assertNull(RangerCommonEnums.getLabelFor_ActivationStatus(8));
    }

    @Test
    public void testGetLabelFor_BooleanValue() {
        assertEquals("None", RangerCommonEnums.getLabelFor_BooleanValue(0));
        assertEquals("True", RangerCommonEnums.getLabelFor_BooleanValue(1));
        assertEquals("False", RangerCommonEnums.getLabelFor_BooleanValue(2));

        // Invalid values
        assertNull(RangerCommonEnums.getLabelFor_BooleanValue(-1));
        assertNull(RangerCommonEnums.getLabelFor_BooleanValue(3));
    }

    @Test
    public void testGetLabelFor_DataType() {
        assertEquals("Unknown", RangerCommonEnums.getLabelFor_DataType(0));
        assertEquals("Integer", RangerCommonEnums.getLabelFor_DataType(1));
        assertEquals("Double", RangerCommonEnums.getLabelFor_DataType(2));
        assertEquals("String", RangerCommonEnums.getLabelFor_DataType(3));
        assertEquals("Boolean", RangerCommonEnums.getLabelFor_DataType(4));
        assertEquals("Date", RangerCommonEnums.getLabelFor_DataType(5));
        assertEquals("String enumeration", RangerCommonEnums.getLabelFor_DataType(6));
        assertEquals("Long", RangerCommonEnums.getLabelFor_DataType(7));
        assertEquals("Integer enumeration", RangerCommonEnums.getLabelFor_DataType(8));

        // Invalid values
        assertNull(RangerCommonEnums.getLabelFor_DataType(-1));
        assertNull(RangerCommonEnums.getLabelFor_DataType(9));
    }

    @Test
    public void testGetLabelFor_DeviceType() {
        assertEquals("Unknown", RangerCommonEnums.getLabelFor_DeviceType(0));
        assertEquals("Browser", RangerCommonEnums.getLabelFor_DeviceType(1));
        assertEquals("iPhone", RangerCommonEnums.getLabelFor_DeviceType(2));
        assertEquals("iPad", RangerCommonEnums.getLabelFor_DeviceType(3));
        assertEquals("iPod", RangerCommonEnums.getLabelFor_DeviceType(4));
        assertEquals("Android", RangerCommonEnums.getLabelFor_DeviceType(5));

        // Invalid/Edge cases
        assertNull(RangerCommonEnums.getLabelFor_DeviceType(-1));
        assertNull(RangerCommonEnums.getLabelFor_DeviceType(6));
    }

    @Test
    public void testGetLabelFor_DiffLevel() {
        assertEquals("Unknown", RangerCommonEnums.getLabelFor_DiffLevel(0));
        assertEquals("Low", RangerCommonEnums.getLabelFor_DiffLevel(1));
        assertEquals("Medium", RangerCommonEnums.getLabelFor_DiffLevel(2));
        assertEquals("High", RangerCommonEnums.getLabelFor_DiffLevel(3));

        // Invalid/Edge cases
        assertNull(RangerCommonEnums.getLabelFor_DiffLevel(-1));
        assertNull(RangerCommonEnums.getLabelFor_DiffLevel(4));
    }

    @Test
    public void testGetLabelFor_FileType() {
        assertEquals("File", RangerCommonEnums.getLabelFor_FileType(0));
        assertEquals("Directory", RangerCommonEnums.getLabelFor_FileType(1));

        // Invalid/Edge cases
        assertNull(RangerCommonEnums.getLabelFor_FileType(-1));
        assertNull(RangerCommonEnums.getLabelFor_FileType(2));
    }

    @Test
    public void testGetLabelFor_FreqType() {
        assertEquals("None", RangerCommonEnums.getLabelFor_FreqType(0));
        assertEquals("Manual", RangerCommonEnums.getLabelFor_FreqType(1));
        assertEquals("Hourly", RangerCommonEnums.getLabelFor_FreqType(2));
        assertEquals("Daily", RangerCommonEnums.getLabelFor_FreqType(3));
        assertEquals("Weekly", RangerCommonEnums.getLabelFor_FreqType(4));
        assertEquals("Bi Weekly", RangerCommonEnums.getLabelFor_FreqType(5));
        assertEquals("Monthly", RangerCommonEnums.getLabelFor_FreqType(6));

        // Edge cases
        assertNull(RangerCommonEnums.getLabelFor_FreqType(-1));
        assertNull(RangerCommonEnums.getLabelFor_FreqType(7));
    }

    @Test
    public void testGetLabelFor_MimeType() {
        assertEquals("Unknown", RangerCommonEnums.getLabelFor_MimeType(0));
        assertEquals("Text", RangerCommonEnums.getLabelFor_MimeType(1));
        assertEquals("Html", RangerCommonEnums.getLabelFor_MimeType(2));
        assertEquals("png", RangerCommonEnums.getLabelFor_MimeType(3));
        assertEquals("jpeg", RangerCommonEnums.getLabelFor_MimeType(4));

        // Edge cases
        assertNull(RangerCommonEnums.getLabelFor_MimeType(-1));
        assertNull(RangerCommonEnums.getLabelFor_MimeType(5));
    }

    @Test
    public void testGetLabelFor_NumberFormat() {
        assertEquals("None", RangerCommonEnums.getLabelFor_NumberFormat(0));
        assertEquals("Numeric", RangerCommonEnums.getLabelFor_NumberFormat(1));
        assertEquals("Alphabhet", RangerCommonEnums.getLabelFor_NumberFormat(2));
        assertEquals("Roman", RangerCommonEnums.getLabelFor_NumberFormat(3));

        // Edge cases
        assertNull(RangerCommonEnums.getLabelFor_NumberFormat(-1));
        assertNull(RangerCommonEnums.getLabelFor_NumberFormat(4));
    }

    @Test
    public void testGetLabelFor_ObjectStatus() {
        assertEquals("Active", RangerCommonEnums.getLabelFor_ObjectStatus(0));
        assertEquals("Deleted", RangerCommonEnums.getLabelFor_ObjectStatus(1));
        assertEquals("Archived", RangerCommonEnums.getLabelFor_ObjectStatus(2));

        // Edge cases
        assertNull(RangerCommonEnums.getLabelFor_ObjectStatus(-1));
        assertNull(RangerCommonEnums.getLabelFor_ObjectStatus(3));
    }

    @Test
    public void testGetLabelFor_PasswordResetStatus() {
        assertEquals("Active", RangerCommonEnums.getLabelFor_PasswordResetStatus(0));
        assertEquals("Used", RangerCommonEnums.getLabelFor_PasswordResetStatus(1));
        assertEquals("Expired", RangerCommonEnums.getLabelFor_PasswordResetStatus(2));
        assertEquals("Disabled", RangerCommonEnums.getLabelFor_PasswordResetStatus(3));

        assertNull(RangerCommonEnums.getLabelFor_PasswordResetStatus(-1));
        assertNull(RangerCommonEnums.getLabelFor_PasswordResetStatus(4));
    }

    @Test
    public void testGetLabelFor_PriorityType() {
        assertEquals("Normal", RangerCommonEnums.getLabelFor_PriorityType(0));
        assertEquals("Low", RangerCommonEnums.getLabelFor_PriorityType(1));
        assertEquals("Medium", RangerCommonEnums.getLabelFor_PriorityType(2));
        assertEquals("High", RangerCommonEnums.getLabelFor_PriorityType(3));

        assertNull(RangerCommonEnums.getLabelFor_PriorityType(-1));
        assertNull(RangerCommonEnums.getLabelFor_PriorityType(4));
    }

    @Test
    public void testGetLabelFor_ProgressStatus() {
        assertEquals("Pending", RangerCommonEnums.getLabelFor_ProgressStatus(0));
        assertEquals("In Progress", RangerCommonEnums.getLabelFor_ProgressStatus(1));
        assertEquals("Complete", RangerCommonEnums.getLabelFor_ProgressStatus(2));
        assertEquals("Aborted", RangerCommonEnums.getLabelFor_ProgressStatus(3));
        assertEquals("Failed", RangerCommonEnums.getLabelFor_ProgressStatus(4));

        assertNull(RangerCommonEnums.getLabelFor_ProgressStatus(-1));
        assertNull(RangerCommonEnums.getLabelFor_ProgressStatus(5));
    }

    @Test
    public void testGetLabelFor_RelationType() {
        assertEquals("None", RangerCommonEnums.getLabelFor_RelationType(0));
        assertEquals("Self", RangerCommonEnums.getLabelFor_RelationType(1));

        assertNull(RangerCommonEnums.getLabelFor_RelationType(-1));
        assertNull(RangerCommonEnums.getLabelFor_RelationType(2));
    }

    @Test
    public void testGetLabelFor_UserSource() {
        assertEquals("Application", RangerCommonEnums.getLabelFor_UserSource(0));
        assertEquals("External", RangerCommonEnums.getLabelFor_UserSource(1));

        // Commented-out values like Google/Facebook are not active in the method
        assertNull(RangerCommonEnums.getLabelFor_UserSource(-1));
        assertNull(RangerCommonEnums.getLabelFor_UserSource(2));
        assertNull(RangerCommonEnums.getLabelFor_UserSource(100));
    }

    @Test
    public void testGetLabelFor_AssetType() {
        assertEquals("Unknown", RangerCommonEnums.getLabelFor_AssetType(0));
        assertEquals("HDFS", RangerCommonEnums.getLabelFor_AssetType(1));
        assertEquals("HBase", RangerCommonEnums.getLabelFor_AssetType(2));
        assertEquals("Hive", RangerCommonEnums.getLabelFor_AssetType(3));
        assertEquals("Agent", RangerCommonEnums.getLabelFor_AssetType(4));
        assertEquals("Knox", RangerCommonEnums.getLabelFor_AssetType(5));
        assertEquals("Storm", RangerCommonEnums.getLabelFor_AssetType(6));

        assertNull(RangerCommonEnums.getLabelFor_AssetType(-1));
        assertNull(RangerCommonEnums.getLabelFor_AssetType(7));
    }

    @Test
    public void testGetLabelFor_AccessResult() {
        assertEquals("Denied", RangerCommonEnums.getLabelFor_AccessResult(0));
        assertEquals("Allowed", RangerCommonEnums.getLabelFor_AccessResult(1));

        assertNull(RangerCommonEnums.getLabelFor_AccessResult(-1));
        assertNull(RangerCommonEnums.getLabelFor_AccessResult(2));
    }

    @Test
    public void testGetLabelFor_PolicyType() {
        assertEquals("Inclusion", RangerCommonEnums.getLabelFor_PolicyType(0));
        assertEquals("Exclusion", RangerCommonEnums.getLabelFor_PolicyType(1));

        assertNull(RangerCommonEnums.getLabelFor_PolicyType(-1));
        assertNull(RangerCommonEnums.getLabelFor_PolicyType(2));
    }

    @Test
    public void testGetLabelFor_XAAuditType() {
        assertEquals("Unknown", RangerCommonEnums.getLabelFor_XAAuditType(0));
        assertEquals("All", RangerCommonEnums.getLabelFor_XAAuditType(1));
        assertEquals("Read", RangerCommonEnums.getLabelFor_XAAuditType(2));
        assertEquals("Write", RangerCommonEnums.getLabelFor_XAAuditType(3));
        assertEquals("Create", RangerCommonEnums.getLabelFor_XAAuditType(4));
        assertEquals("Delete", RangerCommonEnums.getLabelFor_XAAuditType(5));
        assertEquals("Login", RangerCommonEnums.getLabelFor_XAAuditType(6));

        assertNull(RangerCommonEnums.getLabelFor_XAAuditType(-1));
        assertNull(RangerCommonEnums.getLabelFor_XAAuditType(7));
    }

    @Test
    public void testGetLabelFor_ResourceType() {
        assertEquals("Unknown", RangerCommonEnums.getLabelFor_ResourceType(0));
        assertEquals("Path", RangerCommonEnums.getLabelFor_ResourceType(1));
        assertEquals("Database", RangerCommonEnums.getLabelFor_ResourceType(2));
        assertEquals("Table", RangerCommonEnums.getLabelFor_ResourceType(3));
        assertEquals("Column Family", RangerCommonEnums.getLabelFor_ResourceType(4));
        assertEquals("Column", RangerCommonEnums.getLabelFor_ResourceType(5));
        assertEquals("VIEW", RangerCommonEnums.getLabelFor_ResourceType(6));
        assertEquals("UDF", RangerCommonEnums.getLabelFor_ResourceType(7));
        assertEquals("View Column", RangerCommonEnums.getLabelFor_ResourceType(8));

        assertNull(RangerCommonEnums.getLabelFor_ResourceType(-1));
        assertNull(RangerCommonEnums.getLabelFor_ResourceType(9));
    }

    @Test
    public void testGetLabelFor_XAGroupType() {
        assertEquals("Unknown", RangerCommonEnums.getLabelFor_XAGroupType(0));
        assertEquals("User", RangerCommonEnums.getLabelFor_XAGroupType(1));
        assertEquals("Group", RangerCommonEnums.getLabelFor_XAGroupType(2));
        assertEquals("Role", RangerCommonEnums.getLabelFor_XAGroupType(3));

        assertNull(RangerCommonEnums.getLabelFor_XAGroupType(-1));
        assertNull(RangerCommonEnums.getLabelFor_XAGroupType(4));
    }

    @Test
    public void testGetLabelFor_XAPermForType() {
        assertEquals("Unknown", RangerCommonEnums.getLabelFor_XAPermForType(0));
        assertEquals("Permission for Users", RangerCommonEnums.getLabelFor_XAPermForType(1));
        assertEquals("Permission for Groups", RangerCommonEnums.getLabelFor_XAPermForType(2));
        assertNull(RangerCommonEnums.getLabelFor_XAPermForType(3));
        assertNull(RangerCommonEnums.getLabelFor_XAPermForType(-1));
    }

    @Test
    public void testGetLabelFor_XAPermType() {
        assertEquals("Unknown", RangerCommonEnums.getLabelFor_XAPermType(0));
        assertEquals("Reset", RangerCommonEnums.getLabelFor_XAPermType(1));
        assertEquals("Read", RangerCommonEnums.getLabelFor_XAPermType(2));
        assertEquals("Write", RangerCommonEnums.getLabelFor_XAPermType(3));
        assertEquals("Create", RangerCommonEnums.getLabelFor_XAPermType(4));
        assertEquals("Delete", RangerCommonEnums.getLabelFor_XAPermType(5));
        assertEquals("Admin", RangerCommonEnums.getLabelFor_XAPermType(6));
        assertEquals("Obfuscate", RangerCommonEnums.getLabelFor_XAPermType(7));
        assertEquals("Mask", RangerCommonEnums.getLabelFor_XAPermType(8));
        assertEquals("Execute", RangerCommonEnums.getLabelFor_XAPermType(9));
        assertEquals("Select", RangerCommonEnums.getLabelFor_XAPermType(10));
        assertEquals("Update", RangerCommonEnums.getLabelFor_XAPermType(11));
        assertEquals("Drop", RangerCommonEnums.getLabelFor_XAPermType(12));
        assertEquals("Alter", RangerCommonEnums.getLabelFor_XAPermType(13));
        assertEquals("Index", RangerCommonEnums.getLabelFor_XAPermType(14));
        assertEquals("Lock", RangerCommonEnums.getLabelFor_XAPermType(15));
        assertEquals("All", RangerCommonEnums.getLabelFor_XAPermType(16));
        assertEquals("Allow", RangerCommonEnums.getLabelFor_XAPermType(17));
        assertNull(RangerCommonEnums.getLabelFor_XAPermType(18));
        assertNull(RangerCommonEnums.getLabelFor_XAPermType(-1));
    }

    @Test
    public void testGetLabelFor_ClassTypes() {
        assertEquals("None", RangerCommonEnums.getLabelFor_ClassTypes(0));
        assertEquals("Message", RangerCommonEnums.getLabelFor_ClassTypes(1));
        assertEquals("User Profile", RangerCommonEnums.getLabelFor_ClassTypes(2));
        assertEquals("Authentication Session", RangerCommonEnums.getLabelFor_ClassTypes(3));
        assertNull(RangerCommonEnums.getLabelFor_ClassTypes(4));
        assertNull(RangerCommonEnums.getLabelFor_ClassTypes(5));
        assertNull(RangerCommonEnums.getLabelFor_ClassTypes(6));
        assertNull(RangerCommonEnums.getLabelFor_ClassTypes(7));
        assertNull(RangerCommonEnums.getLabelFor_ClassTypes(8));
        assertNull(RangerCommonEnums.getLabelFor_ClassTypes(9));
        assertNull(RangerCommonEnums.getLabelFor_ClassTypes(10));
        assertEquals("Response", RangerCommonEnums.getLabelFor_ClassTypes(11));
        assertEquals("Asset", RangerCommonEnums.getLabelFor_ClassTypes(1000));
        assertEquals("Resource", RangerCommonEnums.getLabelFor_ClassTypes(1001));
        assertEquals("XA Group", RangerCommonEnums.getLabelFor_ClassTypes(1002));
        assertEquals("XA User", RangerCommonEnums.getLabelFor_ClassTypes(1003));
        assertEquals("XA Group of Users", RangerCommonEnums.getLabelFor_ClassTypes(1004));
        assertEquals("XA Group of groups", RangerCommonEnums.getLabelFor_ClassTypes(1005));
        assertEquals("XA permissions for resource", RangerCommonEnums.getLabelFor_ClassTypes(1006));
        assertEquals("XA audits for resource", RangerCommonEnums.getLabelFor_ClassTypes(1007));
        assertEquals("XA credential store", RangerCommonEnums.getLabelFor_ClassTypes(1008));
        assertEquals("XA Policy Export Audit", RangerCommonEnums.getLabelFor_ClassTypes(1009));
        assertEquals("Transaction log", RangerCommonEnums.getLabelFor_ClassTypes(1010));
        assertEquals("Access Audit", RangerCommonEnums.getLabelFor_ClassTypes(1011));
        assertEquals("Transaction log attribute", RangerCommonEnums.getLabelFor_ClassTypes(1012));
        assertNull(RangerCommonEnums.getLabelFor_ClassTypes(1013));
        assertNull(RangerCommonEnums.getLabelFor_ClassTypes(-10));
    }
}
