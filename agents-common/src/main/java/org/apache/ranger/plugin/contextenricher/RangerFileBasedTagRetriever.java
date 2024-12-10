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

package org.apache.ranger.plugin.contextenricher;

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.authorization.utils.JsonUtils;
import org.apache.ranger.plugin.util.ServiceTags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class RangerFileBasedTagRetriever extends RangerTagRetriever {
    private static final Logger LOG = LoggerFactory.getLogger(RangerFileBasedTagRetriever.class);

    int     tagFilesCount;
    int     currentTagFileIndex;
    boolean isInitial = true;

    private URL    serviceTagsFileURL;
    private String serviceTagsFileName;

    @Override
    public void init(Map<String, String> options) {
        LOG.debug("==> init()");

        String serviceTagsFileNameProperty = "serviceTagsFileName";
        String serviceTagsDefaultFileName  = "/testdata/test_servicetags_hive.json";
        String tagFilesCountProperty       = "tagFileCount";

        if (StringUtils.isNotBlank(serviceName) && serviceDef != null && StringUtils.isNotBlank(appId)) {
            // Open specified file from options- it should contain service-tags
            serviceTagsFileName = options != null ? options.get(serviceTagsFileNameProperty) : null;
            serviceTagsFileName = serviceTagsFileName == null ? serviceTagsDefaultFileName : serviceTagsFileName;

            if (options != null) {
                String tagFilesCountStr = options.get(tagFilesCountProperty);

                if (!StringUtils.isNotEmpty(tagFilesCountStr)) {
                    try {
                        tagFilesCount = Integer.parseInt(tagFilesCountStr);
                    } catch (Exception e) {
                        LOG.error("Exception while parsing tagFileCount option value:[{}]", tagFilesCountStr);
                        LOG.error("Setting tagFilesCount to 0");
                    }
                }
            }

            if (StringUtils.isNotBlank(serviceTagsFileName)) {
                serviceTagsFileURL = getTagFileURL(serviceTagsFileName);
            }

            isInitial = true;
        } else {
            LOG.error("FATAL: Cannot find service/serviceDef/serviceTagsFile to use for retrieving tags. Will NOT be able to retrieve tags.");
        }

        LOG.debug("<== init() : serviceTagsFileName={}", serviceTagsFileName);
    }

    @Override
    public ServiceTags retrieveTags(long lastKnownVersion, long lastActivationTimeInMillis) {
        LOG.debug("==> retrieveTags(lastKnownVersion={}, lastActivationTimeInMillis={}, serviceTagsFilePath={}", lastKnownVersion, lastActivationTimeInMillis, serviceTagsFileName);

        ServiceTags serviceTags = readFromFile();

        LOG.debug("<== retrieveTags(lastKnownVersion={}, lastActivationTimeInMillis={}", lastKnownVersion, lastActivationTimeInMillis);

        return serviceTags;
    }

    URL getTagFileURL(String fileName) {
        URL         fileURL       = null;
        InputStream tagFileStream = null;
        File        f             = new File(fileName);

        if (f.exists() && f.isFile() && f.canRead()) {
            try {
                tagFileStream = new FileInputStream(f);
                fileURL       = f.toURI().toURL();
            } catch (FileNotFoundException exception) {
                LOG.error("Error processing input file:{} or no privilege for reading file {}", fileName, fileName, exception);
            } catch (MalformedURLException malformedException) {
                LOG.error("Error processing input file:{} cannot be converted to URL {}", fileName, fileName, malformedException);
            }
        } else {
            fileURL = getClass().getResource(fileName);

            if (fileURL == null) {
                if (!fileName.startsWith("/")) {
                    fileURL = getClass().getResource("/" + fileName);
                }
            }

            if (fileURL == null) {
                fileURL = ClassLoader.getSystemClassLoader().getResource(fileName);

                if (fileURL == null) {
                    if (!fileName.startsWith("/")) {
                        fileURL = ClassLoader.getSystemClassLoader().getResource("/" + fileName);
                    }
                }
            }

            if (fileURL != null) {
                try {
                    tagFileStream = fileURL.openStream();
                } catch (Exception exception) {
                    fileURL = null;

                    LOG.error("{} is not a file", fileName, exception);
                }
            } else {
                LOG.warn("Error processing input file: URL not found for {} or no privilege for reading file {}", fileName, fileName);
            }
        }

        if (tagFileStream != null) {
            try {
                tagFileStream.close();
            } catch (Exception e) {
                // Ignore
            }
        }

        return fileURL;
    }

    private ServiceTags readFromFile() {
        LOG.debug("==> RangerFileBasedTagRetriever.readFromFile: sourceFileName={}", serviceTagsFileName);

        ServiceTags ret      = null;
        String      fileName = serviceTagsFileName;

        if (isInitial) {
            isInitial = false;

            if (serviceTagsFileURL != null) {
                try (InputStream fileStream = serviceTagsFileURL.openStream(); Reader reader = new InputStreamReader(fileStream, StandardCharsets.UTF_8)) {
                    ret = JsonUtils.jsonToObject(reader, ServiceTags.class);

                    if (ret.getIsTagsDeduped()) {
                        final int countOfDuplicateTags = ret.dedupTags();

                        LOG.info("Number of duplicate tags removed from the received serviceTags:[{}]. Number of tags in the de-duplicated serviceTags :[{}].", countOfDuplicateTags, ret.getTags().size());
                    }
                } catch (IOException e) {
                    LOG.warn("Error processing input file: or no privilege for reading file {}", fileName, e);
                }
            } else {
                LOG.error("Error reading file: {}", fileName);
            }
        } else if (tagFilesCount > 0) {
            currentTagFileIndex = currentTagFileIndex % tagFilesCount;
            fileName            = serviceTagsFileName + "_" + currentTagFileIndex + ".json";

            URL fileURL = getTagFileURL(fileName);

            if (fileURL != null) {
                try (InputStream fileStream = fileURL.openStream(); Reader reader = new InputStreamReader(fileStream, StandardCharsets.UTF_8)) {
                    ret = JsonUtils.jsonToObject(reader, ServiceTags.class);

                    currentTagFileIndex++;

                    if (ret.getIsTagsDeduped()) {
                        final int countOfDuplicateTags = ret.dedupTags();

                        LOG.info("Number of duplicate tags removed from the received serviceTags:[{}]. Number of tags in the de-duplicated serviceTags :[{}].", countOfDuplicateTags, ret.getTags().size());
                    }
                } catch (IOException e) {
                    LOG.warn("Error processing input file: or no privilege for reading file {}", fileName, e);
                }
            } else {
                LOG.error("Error reading file: {}", fileName);
            }
        }

        LOG.debug("<== RangerFileBasedTagRetriever.readFromFile: sourceFileName={}", fileName);

        return ret;
    }
}
