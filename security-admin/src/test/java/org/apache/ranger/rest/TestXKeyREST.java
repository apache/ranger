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
package org.apache.ranger.rest;

import org.apache.ranger.biz.KmsKeyMgr;
import org.apache.ranger.biz.XAuditMgr;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.SearchUtil;
import org.apache.ranger.service.XAccessAuditService;
import org.apache.ranger.view.VXKmsKey;
import org.apache.ranger.view.VXKmsKeyList;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.WebApplicationException;

@RunWith(MockitoJUnitRunner.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestXKeyREST {
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    @InjectMocks
    XKeyREST keyREST = new XKeyREST();
    @Mock
    XAuditMgr xAuditMgr;
    @Mock
    SearchUtil searchUtil;
    @Mock
    XAccessAuditService xAccessAuditSrv;
    @Mock
    KmsKeyMgr keyMgr;
    @Mock
    VXKmsKey vxKmsKey;
    @Mock
    RESTErrorUtil restErrorUtil;
    @Mock
    HttpServletRequest request;
    String provider = "providerX";
    String name     = "xyz";
    String nameNl   = "";

    @Test
    public void test1Searchkeys() throws Exception {
        VXKmsKeyList vxKeyListExp = new VXKmsKeyList();

        Mockito.when(keyMgr.searchKeys(request, provider)).thenReturn(vxKeyListExp);

        VXKmsKeyList vxKeyListAct = keyREST.searchKeys(request, provider);

        Assert.assertNotNull(vxKeyListAct);
        Assert.assertEquals(vxKeyListExp, vxKeyListAct);

        Mockito.verify(keyMgr).searchKeys(request, provider);
    }

    @Test
    public void test2RolloverKey() throws Exception {
        VXKmsKey vxKeyExp = new VXKmsKey();
        vxKeyExp.setName(name);
        vxKeyExp.setCipher("CipherX");

        Mockito.when(keyMgr.rolloverKey(provider, vxKeyExp)).thenReturn(vxKeyExp);

        VXKmsKey vxKeyAct = keyREST.rolloverKey(provider, vxKeyExp);

        Assert.assertNotNull(vxKeyAct);
        Assert.assertEquals(vxKeyExp, vxKeyAct);
        Assert.assertEquals(vxKeyExp.getName(), vxKeyAct.getName());
        Mockito.verify(keyMgr).rolloverKey(provider, vxKeyExp);
    }

    @Test
    public void test3RolloverKey() throws Exception {
        VXKmsKey vxKeyExp = new VXKmsKey();
        vxKeyExp.setName(name);

        Mockito.when(keyMgr.rolloverKey(provider, vxKeyExp)).thenReturn(vxKeyExp);

        VXKmsKey vxKeyAct = keyREST.rolloverKey(provider, vxKeyExp);

        Assert.assertNotNull(vxKeyAct);
        Assert.assertEquals(vxKeyExp, vxKeyAct);
        Assert.assertEquals(vxKeyExp.getName(), vxKeyAct.getName());
        Assert.assertNull(vxKeyAct.getCipher());

        Mockito.verify(keyMgr).rolloverKey(provider, vxKeyExp);
    }

    @Test
    public void test4RolloverKey()  {
        VXKmsKey vxKeyExp = new VXKmsKey();

        Mockito.when(restErrorUtil.createRESTException(Mockito.nullable(String.class), Mockito.any()))
                .thenReturn(new WebApplicationException());
        thrown.expect(WebApplicationException.class);

        keyREST.rolloverKey(provider, vxKeyExp);

        Mockito.verify(restErrorUtil).createRESTException(Mockito.anyString(), Mockito.any());
    }

    @Test
    public void test5DeleteKey() throws Exception {
        Mockito.doNothing().when(keyMgr).deleteKey(provider, name);

        keyREST.deleteKey(name, provider, request);

        Mockito.verify(keyMgr).deleteKey(provider, name);
    }

    @Test
    public void test6DeleteKey() {
        Mockito.when(restErrorUtil.createRESTException(Mockito.nullable(String.class), Mockito.any())).thenReturn(new WebApplicationException());

        thrown.expect(WebApplicationException.class);

        keyREST.deleteKey(nameNl, provider, request);

        Mockito.verify(restErrorUtil).createRESTException(Mockito.anyString(), Mockito.any());
    }

    @Test
    public void test6CreateKey() throws Exception {
        VXKmsKey vxKeyExp = new VXKmsKey();
        vxKeyExp.setName(name);
        vxKeyExp.setCipher("CipherX");

        Mockito.when(keyMgr.createKey(provider, vxKeyExp)).thenReturn(vxKeyExp);
        VXKmsKey vxKeyAct = keyREST.createKey(provider, vxKeyExp);

        Assert.assertNotNull(vxKeyAct);
        Assert.assertEquals(vxKeyAct, vxKeyExp);
        Assert.assertEquals(vxKeyExp.getName(), vxKeyAct.getName());
        Assert.assertEquals(vxKeyExp.getCipher(), vxKeyAct.getCipher());

        Mockito.verify(keyMgr).createKey(provider, vxKeyExp);
    }

    @Test
    public void test7CreateKey() throws Exception {
        VXKmsKey vxKeyExp = new VXKmsKey();
        vxKeyExp.setName(name);

        Mockito.when(keyMgr.createKey(provider, vxKeyExp)).thenReturn(vxKeyExp);

        VXKmsKey vxKeyAct = keyREST.createKey(provider, vxKeyExp);

        Assert.assertNotNull(vxKeyAct);
        Assert.assertEquals(vxKeyAct, vxKeyExp);
        Assert.assertEquals(vxKeyExp.getName(), vxKeyAct.getName());
        Assert.assertNull(vxKeyAct.getCipher());

        Mockito.verify(keyMgr).createKey(provider, vxKeyExp);
    }

    @Test
    public void test8CreateKey() {
        VXKmsKey vxKeyExp = new VXKmsKey();

        Mockito.when(restErrorUtil.createRESTException(Mockito.nullable(String.class), Mockito.any())).thenReturn(new WebApplicationException());
        thrown.expect(WebApplicationException.class);

        VXKmsKey vxKeyAct = keyREST.createKey(provider, vxKeyExp);

        Mockito.verify(restErrorUtil).createRESTException(Mockito.anyString(), Mockito.any());

        Assert.assertNull(vxKeyAct);
    }

    @Test
    public void test9GetKey() throws Exception {
        VXKmsKey vxKeyExp = new VXKmsKey();

        Mockito.when(keyMgr.getKey(provider, name)).thenReturn(vxKeyExp);

        VXKmsKey vxKeyAct = keyREST.getKey(name, provider);

        Assert.assertNotNull(vxKeyAct);
        Assert.assertEquals(vxKeyAct, vxKeyExp);
        Assert.assertEquals(vxKeyExp.getName(), vxKeyAct.getName());

        Mockito.verify(keyMgr).getKey(provider, name);
    }

    @Test
    public void test10GetKey() {
        Mockito.when(restErrorUtil.createRESTException(Mockito.nullable(String.class), Mockito.any())).thenReturn(new WebApplicationException());
        thrown.expect(WebApplicationException.class);

        VXKmsKey vxKeyAct = keyREST.getKey(nameNl, provider);

        Assert.assertNull(vxKeyAct);

        Mockito.verify(restErrorUtil).createRESTException(Mockito.anyString(), Mockito.any());
    }
}
