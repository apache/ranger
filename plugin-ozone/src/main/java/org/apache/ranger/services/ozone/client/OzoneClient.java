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

package org.apache.ranger.services.ozone.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.ranger.plugin.client.BaseClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.*;

public class OzoneClient extends BaseClient {

    private static final Log LOG        = LogFactory.getLog(OzoneClient.class);
    private static final String ERR_MSG = "You can still save the repository and start creating policies, but you " +
            "would not be able to use autocomplete for resource names. Check ranger_admin.log for more info.";

    private final OzoneConfiguration conf;
    private org.apache.hadoop.ozone.client.OzoneClient ozoneClient = null;

    public OzoneClient(String serviceName, Map<String,String> connectionProperties) throws Exception{
        super(serviceName,connectionProperties, "ozone-client");
        conf = new OzoneConfiguration();
        Set<String> rangerInternalPropertyKeys = getConfigHolder().getRangerInternalPropertyKeys();
        for (Map.Entry<String, String> entry: connectionProperties.entrySet())  {
            String key   = entry.getKey();
            String value = entry.getValue();
            if (!rangerInternalPropertyKeys.contains(key) && value != null) {
                conf.set(key, value);
            }
        }
        Subject.doAs(getLoginSubject(), (PrivilegedExceptionAction<Void>) () -> {
            String[] serviceIds = conf.getTrimmedStrings("ozone.om.service.ids", "ozone1");
            ozoneClient = OzoneClientFactory.getRpcClient(serviceIds[0], conf);
            return null;
        });
    }

    public void close() {
        try {
            ozoneClient.close();
        } catch (IOException e) {
            LOG.error("Unable to close Ozone Client connection", e);
        }

    }

    public List<String> getVolumeList(String volumePrefix) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> OzoneClient getVolume volumePrefix : " + volumePrefix);
        }

        List<String> ret = new ArrayList<>();
        try {
            if (ozoneClient != null) {
                Iterator<? extends OzoneVolume> ozoneVolList = ozoneClient.getObjectStore().listVolumes(volumePrefix);
                if (ozoneVolList != null) {
                    while (ozoneVolList.hasNext()) {
                        ret.add(ozoneVolList.next().getName());
                    }
                }
            }
        } catch (IOException e) {
            LOG.error("Unable to get Volume List");
            if (LOG.isDebugEnabled()) {
                LOG.debug("<== OzoneClient.getVolumeList() Error : " , e);
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== OzoneClient.getVolumeList(): " + ret);
        }
        return ret;
    }

    public List<String> getBucketList(String bucketPrefix, List<String> finalVolumeList) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> OzoneClient getBucketList bucketPrefix : " + bucketPrefix);
        }
        List<String> ret = new ArrayList<>();
        try {
            if (ozoneClient != null && finalVolumeList != null && !finalVolumeList.isEmpty()){
                for (String ozoneVol : finalVolumeList) {
                    Iterator<? extends OzoneBucket> ozoneBucketList = ozoneClient.getObjectStore().getVolume(ozoneVol).listBuckets(bucketPrefix);
                    if (ozoneBucketList != null) {
                        while (ozoneBucketList.hasNext()) {
                            ret.add(ozoneBucketList.next().getName());
                        }
                    }
                }
            }
        } catch (IOException e) {
            LOG.error("Unable to get Volume List");
            if (LOG.isDebugEnabled()) {
                LOG.debug("<== OzoneClient.getVolumeList() Error : " , e);
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== OzoneClient.getVolumeList(): " + ret);
        }
        return ret;
    }

    public List<String> getKeyList(String keyPrefix, List<String> finalVolumeList, List<String> finalBucketList) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> OzoneClient getKeyList keyPrefix : " + keyPrefix);
        }
        List<String> ret = new ArrayList<>();
        try {
            if (ozoneClient != null && finalVolumeList != null && !finalVolumeList.isEmpty()) {
                for (String ozoneVol : finalVolumeList) {
                    Iterator<? extends OzoneBucket> ozoneBucketIterator = ozoneClient.getObjectStore().getVolume(ozoneVol).listBuckets(null);
                    if (ozoneBucketIterator != null) {
                        while (ozoneBucketIterator.hasNext()) {
                            OzoneBucket currentBucket = ozoneBucketIterator.next();
                            if (finalBucketList.contains(currentBucket.getName())) {
                                Iterator<? extends OzoneKey> ozoneKeyIterator = currentBucket.listKeys(keyPrefix);
                                if (ozoneKeyIterator != null) {
                                    while (ozoneKeyIterator.hasNext()) {
                                        ret.add(ozoneKeyIterator.next().getName());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } catch (IOException e) {
            LOG.error("Unable to get Volume List");
            if (LOG.isDebugEnabled()) {
                LOG.debug("<== OzoneClient.getVolumeList() Error : " , e);
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== OzoneClient.getVolumeList(): " + ret);
        }
        return ret;
    }

    public static Map<String, Object> connectionTest(String serviceName,
                                                     Map<String, String> connectionProperties) throws Exception {
        Map<String, Object> responseData = new HashMap<>();
        OzoneClient connectionObj = null;
        boolean connectivityStatus = false;
        List<String> testResult;

        try {
            connectionObj = new OzoneClient(serviceName, connectionProperties);
            testResult    = connectionObj.getVolumeList("");
            if (testResult != null && testResult.size() != 0) {
                connectivityStatus = true;
            }
            if (connectivityStatus) {
                String successMsg = "ConnectionTest Successful";
                generateResponseDataMap(true, successMsg, successMsg,
                        null, null, responseData);
            } else {
                String failureMsg = "Unable to retrieve any volumes using given parameters.";
                generateResponseDataMap(false, failureMsg, failureMsg + ERR_MSG,
                        null, null, responseData);
            }
        } finally {
            if (connectionObj != null) {
                connectionObj.close();
            }
        }

        return responseData;
    }
}
