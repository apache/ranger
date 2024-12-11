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

package org.apache.ranger.tagsync.source.file;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.authorization.utils.JsonUtils;
import org.apache.ranger.plugin.util.ServiceTags;
import org.apache.ranger.tagsync.model.AbstractTagSource;
import org.apache.ranger.tagsync.model.TagSink;
import org.apache.ranger.tagsync.process.TagSyncConfig;
import org.apache.ranger.tagsync.process.TagSynchronizer;
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
import java.util.Date;
import java.util.Properties;

public class FileTagSource extends AbstractTagSource implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(FileTagSource.class);

    private String serviceTagsFileName;
    private URL    serviceTagsFileURL;
    private long   lastModifiedTimeInMillis = -1L;

    private long fileModTimeCheckIntervalInMs;

    private Thread myThread;

    public static void main(String[] args) {
        FileTagSource fileTagSource = new FileTagSource();

        TagSyncConfig config = TagSyncConfig.getInstance();

        Properties props = config.getProperties();

        if (args.length > 0) {
            String tagSourceFileName = args[0];
            LOG.info("TagSourceFileName is set to {}", args[0]);
            props.setProperty(TagSyncConfig.TAGSYNC_FILESOURCE_FILENAME_PROP, tagSourceFileName);
        }

        TagSynchronizer.printConfigurationProperties(props);

        boolean ret = TagSynchronizer.initializeKerberosIdentity(props);

        if (ret) {
            TagSink tagSink = TagSynchronizer.initializeTagSink(props);

            if (tagSink != null) {
                if (fileTagSource.initialize(props)) {
                    try {
                        tagSink.start();
                        fileTagSource.setTagSink(tagSink);
                        fileTagSource.synchUp();
                    } catch (Exception exception) {
                        LOG.error("ServiceTags upload failed : ", exception);
                        System.exit(1);
                    }
                } else {
                    LOG.error("FileTagSource initialized failed, exiting.");
                    System.exit(1);
                }
            } else {
                LOG.error("TagSink initialialization failed, exiting.");
                System.exit(1);
            }
        } else {
            LOG.error("Error initializing kerberos identity");
            System.exit(1);
        }
    }

    @Override
    public boolean initialize(Properties props) {
        LOG.debug("==> FileTagSource.initialize()");

        Properties properties;

        if (props == null || MapUtils.isEmpty(props)) {
            LOG.error("No properties specified for FileTagSource initialization");
            properties = new Properties();
        } else {
            properties = props;
        }

        boolean ret = true;

        serviceTagsFileName = TagSyncConfig.getTagSourceFileName(properties);

        if (StringUtils.isBlank(serviceTagsFileName)) {
            ret = false;
            LOG.error("value of property 'ranger.tagsync.source.impl.class' is file and no value specified for property 'ranger.tagsync.filesource.filename'!");
        }

        if (ret) {
            fileModTimeCheckIntervalInMs = TagSyncConfig.getTagSourceFileModTimeCheckIntervalInMillis(properties);

            LOG.debug("Provided serviceTagsFileName={}", serviceTagsFileName);
            LOG.debug("'ranger.tagsync.filesource.modtime.check.interval':{}ms", fileModTimeCheckIntervalInMs);

            InputStream serviceTagsFileStream = null;

            File f = new File(serviceTagsFileName);

            if (f.exists() && f.isFile() && f.canRead()) {
                try {
                    serviceTagsFileStream = new FileInputStream(f);
                    serviceTagsFileURL    = f.toURI().toURL();
                } catch (FileNotFoundException exception) {
                    LOG.error("Error processing input file:{} or no privilege for reading file {}", serviceTagsFileName, serviceTagsFileName, exception);
                } catch (MalformedURLException malformedException) {
                    LOG.error("Error processing input file:{} cannot be converted to URL {}", serviceTagsFileName, serviceTagsFileName, malformedException);
                }
            } else {
                URL fileURL = getClass().getResource(serviceTagsFileName);
                if (fileURL == null) {
                    if (!serviceTagsFileName.startsWith("/")) {
                        fileURL = getClass().getResource("/" + serviceTagsFileName);
                    }
                }

                if (fileURL == null) {
                    fileURL = ClassLoader.getSystemClassLoader().getResource(serviceTagsFileName);
                    if (fileURL == null) {
                        if (!serviceTagsFileName.startsWith("/")) {
                            fileURL = ClassLoader.getSystemClassLoader().getResource("/" + serviceTagsFileName);
                        }
                    }
                }

                if (fileURL != null) {
                    try {
                        serviceTagsFileStream = fileURL.openStream();
                        serviceTagsFileURL    = fileURL;
                    } catch (Exception exception) {
                        LOG.error("{} is not a file", serviceTagsFileName, exception);
                    }
                } else {
                    LOG.warn("Error processing input file: URL not found for {} or no privilege for reading file {}", serviceTagsFileName, serviceTagsFileName);
                }
            }

            if (serviceTagsFileStream != null) {
                try {
                    serviceTagsFileStream.close();
                } catch (Exception e) {
                    // Ignore
                }
            }
        }

        LOG.debug("<== FileTagSource.initialize(): sourceFileName={}, result={}", serviceTagsFileName, ret);

        return ret;
    }

    @Override
    public boolean start() {
        myThread = new Thread(this);
        myThread.setDaemon(true);
        myThread.start();

        return true;
    }

    @Override
    public void stop() {
        if (myThread != null && myThread.isAlive()) {
            myThread.interrupt();
        }
    }

    @Override
    public void run() {
        LOG.debug("==> FileTagSource.run()");

        while (true) {
            try {
                if (TagSyncConfig.isTagSyncServiceActive()) {
                    LOG.debug("==> FileTagSource is running as server is active");

                    synchUp();
                }
            } catch (Exception e) {
                LOG.error("Caught exception..", e);
            } finally {
                LOG.debug("Sleeping for [{}] milliSeconds", fileModTimeCheckIntervalInMs);

                try {
                    Thread.sleep(fileModTimeCheckIntervalInMs);
                } catch (InterruptedException exception) {
                    LOG.error("Interrupted..: ", exception);

                    break;
                }
            }
        }
    }

    public void synchUp() throws Exception {
        if (isChanged()) {
            LOG.debug("Begin: update tags from source==>sink");

            ServiceTags serviceTags = readFromFile();

            updateSink(serviceTags);

            lastModifiedTimeInMillis = getModificationTime();

            LOG.debug("End: update tags from source==>sink");
        } else {
            LOG.debug("FileTagSource: no change found for synchronization.");
        }
    }

    private boolean isChanged() {
        LOG.debug("==> FileTagSource.isChanged()");

        boolean ret              = false;
        long    modificationTime = getModificationTime();

        if (modificationTime > lastModifiedTimeInMillis) {
            LOG.debug("File modified at {}last-modified at {}", new Date(modificationTime), new Date(lastModifiedTimeInMillis));

            ret = true;
        }

        LOG.debug("<== FileTagSource.isChanged(): result={}", ret);

        return ret;
    }

    private ServiceTags readFromFile() {
        LOG.debug("==> FileTagSource.readFromFile(): sourceFileName={}", serviceTagsFileName);

        ServiceTags ret = null;

        if (serviceTagsFileURL != null) {
            try (InputStream inputStream = serviceTagsFileURL.openStream();
                    Reader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
                ret = JsonUtils.jsonToObject(reader, ServiceTags.class);
            } catch (IOException e) {
                LOG.warn("Error processing input file: or no privilege for reading file {}", serviceTagsFileName, e);
            }
        } else {
            LOG.error("Error reading file: {}", serviceTagsFileName);
        }

        LOG.debug("<== FileTagSource.readFromFile(): sourceFileName={}", serviceTagsFileName);

        return ret;
    }

    private long getModificationTime() {
        LOG.debug("==> FileTagSource.getLastModificationTime(): sourceFileName={}", serviceTagsFileName);

        long ret        = 0L;
        File sourceFile = new File(serviceTagsFileName);

        if (sourceFile.exists() && sourceFile.isFile() && sourceFile.canRead()) {
            ret = sourceFile.lastModified();
        }

        LOG.debug("<== FileTagSource.lastModificationTime(): serviceTagsFileName={} result={}", serviceTagsFileName, new Date(ret));

        return ret;
    }
}
