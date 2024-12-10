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
import org.apache.ranger.plugin.geo.RangerGeolocationData;
import org.apache.ranger.plugin.geo.RangerGeolocationDatabase;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.store.GeolocationStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public abstract class RangerAbstractGeolocationProvider extends RangerAbstractContextEnricher {
    private static final Logger LOG = LoggerFactory.getLogger(RangerAbstractGeolocationProvider.class);

    public static final String ENRICHER_OPTION_GEOLOCATION_META_PREFIX = "geolocation.meta.prefix";
    public static final String KEY_CONTEXT_GEOLOCATION_PREFIX          = "LOCATION_";

    private GeolocationStore store;
    private String           geoMetaPrefix;

    public abstract String getGeoSourceLoader();

    @Override
    public void init() {
        LOG.debug("==> RangerAbstractGeolocationProvider.init()");

        super.init();

        geoMetaPrefix = getOption(ENRICHER_OPTION_GEOLOCATION_META_PREFIX);

        if (geoMetaPrefix == null) {
            geoMetaPrefix = "";
        }

        GeolocationStore    geoStore = null;
        Map<String, String> context  = enricherDef.getEnricherOptions();

        if (context != null) {
            String geoSourceLoader = getGeoSourceLoader();

            try {
                // Get the class definition and ensure it is of the correct type
                @SuppressWarnings("unchecked")
                Class<GeolocationStore> geoSourceLoaderClass = (Class<GeolocationStore>) Class.forName(geoSourceLoader);
                // instantiate the loader class and initialize it with options
                geoStore = geoSourceLoaderClass.newInstance();
            } catch (ClassNotFoundException exception) {
                LOG.error("RangerAbstractGeolocationProvider.init() - Class {} not found, exception={}", geoSourceLoader, exception.toString());
            } catch (ClassCastException exception) {
                LOG.error("RangerAbstractGeolocationProvider.init() - Class {} is not a type of GeolocationStore, exception={}", geoSourceLoader, exception.toString());
            } catch (IllegalAccessException | InstantiationException exception) {
                LOG.error("RangerAbstractGeolocationProvider.init() - Class {} could not be instantiated, exception={}", geoSourceLoader, exception.toString());
            }

            if (geoStore != null) {
                try {
                    geoStore.init(context);

                    store = geoStore;
                } catch (Exception exception) {
                    LOG.error("RangerAbstractGeolocationProvider.init() - geoLocation Store cannot be initialized, exception={}", exception.toString());
                }
            }
        }

        if (store == null) {
            LOG.error("RangerAbstractGeolocationProvider.init() - is not initialized correctly.");
        }

        LOG.debug("<== RangerAbstractGeolocationProvider.init()");
    }

    @Override
    public void enrich(RangerAccessRequest request) {
        LOG.debug("==> RangerAbstractGeolocationProvider.enrich({})", request);

        RangerGeolocationData geolocation;
        String                clientIPAddress = request.getClientIPAddress();

        LOG.debug("RangerAbstractGeolocationProvider.enrich() - clientIPAddress={}", clientIPAddress);

        if (StringUtils.isNotBlank(clientIPAddress) && store != null) {
            geolocation = store.getGeoLocation(clientIPAddress);

            if (geolocation != null) {
                LOG.debug("RangerAbstractGeolocationProvider.enrich() - Country={}", geolocation);

                Map<String, Object>       context        = request.getContext();
                String[]                  geoAttrValues  = geolocation.getLocationData();
                RangerGeolocationDatabase database       = store.getGeoDatabase();
                String[]                  attributeNames = database.getMetadata().getLocationDataItemNames();

                for (int i = 0; i < geoAttrValues.length && i < attributeNames.length; i++) {
                    String contextName = KEY_CONTEXT_GEOLOCATION_PREFIX + geoMetaPrefix + attributeNames[i];

                    context.put(contextName, geoAttrValues[i]);
                }
            } else {
                LOG.debug("RangerAbstractGeolocationProvider.enrich() - clientIPAddress '{}' not found.", clientIPAddress);
            }
        } else {
            LOG.debug("RangerAbstractGeolocationProvider.enrich() - clientIPAddress is null or blank, cannot get geolocation");
        }

        LOG.debug("<== RangerAbstractGeolocationProvider.enrich({})", request);
    }
}
