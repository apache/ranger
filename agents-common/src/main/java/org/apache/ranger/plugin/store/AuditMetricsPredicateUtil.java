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

package org.apache.ranger.plugin.store;

import org.apache.commons.collections.Predicate;
import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.plugin.model.RangerAuditMetrics;
import org.apache.ranger.plugin.model.RangerAuditMetricsByDays;
import org.apache.ranger.plugin.model.RangerAuditMetricsByHours;
import org.apache.ranger.plugin.util.SearchFilter;

import java.util.List;

public class AuditMetricsPredicateUtil extends AbstractPredicateUtil {
    public AuditMetricsPredicateUtil() {
        super();
    }

    @Override
    public void addPredicates(SearchFilter filter, List<Predicate> predicates) {
        addPredicateForServiceType(filter.getParam(SearchFilter.SERVICE_TYPE), predicates);
        addPredicateForServiceName(filter.getParam(SearchFilter.SERVICE_NAME), predicates);
        addPredicateForAppId(filter.getParam(SearchFilter.APP_ID), predicates);
        addPredicateForClusterName(filter.getParam(SearchFilter.CLUSTER_NAME), predicates);
        addPredicateForClientIP(filter.getParam(SearchFilter.CLIENT_IP), predicates);
    }

    private Predicate addPredicateForServiceType(final String serviceType, List<Predicate> predicates) {
        if (StringUtils.isEmpty(serviceType)) {
            return null;
        }

        Predicate ret = new Predicate() {
            @Override
            public boolean evaluate(Object object) {
                if (object == null) {
                    return false;
                }

                boolean ret = false;

                if (object instanceof RangerAuditMetrics) {
                    RangerAuditMetrics rangerAuditMetrics = (RangerAuditMetrics) object;
                    ret = StringUtils.equals(rangerAuditMetrics.getServiceType().toString(), serviceType);
                } else if (object instanceof RangerAuditMetricsByHours) {
                    RangerAuditMetricsByHours rangerAuditMetricsByHours = (RangerAuditMetricsByHours) object;
                    ret = StringUtils.equals(rangerAuditMetricsByHours.getServiceType().toString(), serviceType);
                } else if (object instanceof RangerAuditMetricsByDays) {
                    RangerAuditMetricsByDays rangerAuditMetricsByDays = (RangerAuditMetricsByDays) object;
                    ret = StringUtils.equals(rangerAuditMetricsByDays.getServiceType().toString(), serviceType);
                }
                return ret;
            }
        };

        if (predicates != null) {
            predicates.add(ret);
        }

        return ret;
    }

    private Predicate addPredicateForServiceName(final String serviceName, List<Predicate> predicates) {
        if (StringUtils.isEmpty(serviceName)) {
            return null;
        }

        Predicate ret = new Predicate() {
            @Override
            public boolean evaluate(Object object) {
                if (object == null) {
                    return false;
                }

                boolean ret = false;

                if (object instanceof RangerAuditMetrics) {
                    RangerAuditMetrics rangerAuditMetrics = (RangerAuditMetrics) object;
                    ret = StringUtils.equals(rangerAuditMetrics.getServiceName(), serviceName);
                } else if (object instanceof RangerAuditMetricsByHours) {
                    RangerAuditMetricsByHours rangerAuditMetricsByHours = (RangerAuditMetricsByHours) object;
                    ret = StringUtils.equals(rangerAuditMetricsByHours.getServiceName(), serviceName);
                } else if (object instanceof RangerAuditMetricsByDays) {
                    RangerAuditMetricsByDays rangerAuditMetricsByDays = (RangerAuditMetricsByDays) object;
                    ret = StringUtils.equals(rangerAuditMetricsByDays.getServiceName(), serviceName);
                }
                return ret;
            }
        };

        if (predicates != null) {
            predicates.add(ret);
        }

        return ret;
    }

    private Predicate addPredicateForAppId(final String appId, List<Predicate> predicates) {
        if (StringUtils.isEmpty(appId)) {
            return null;
        }

        Predicate ret = new Predicate() {
            @Override
            public boolean evaluate(Object object) {
                if (object == null) {
                    return false;
                }

                boolean ret = false;

                if (object instanceof RangerAuditMetrics) {
                    RangerAuditMetrics rangerAuditMetrics = (RangerAuditMetrics) object;
                    ret = StringUtils.equals(rangerAuditMetrics.getAppId(), appId);
                } else if (object instanceof RangerAuditMetricsByHours) {
                    RangerAuditMetricsByHours rangerAuditMetricsByHours = (RangerAuditMetricsByHours) object;
                    ret = StringUtils.equals(rangerAuditMetricsByHours.getAppId(), appId);
                } else if (object instanceof RangerAuditMetricsByDays) {
                    RangerAuditMetricsByDays rangerAuditMetricsByDays = (RangerAuditMetricsByDays) object;
                    ret = StringUtils.equals(rangerAuditMetricsByDays.getAppId(), appId);
                }
                return ret;
            }
        };

        if (predicates != null) {
            predicates.add(ret);
        }

        return ret;
    }

    private Predicate addPredicateForClusterName(final String clusterName, List<Predicate> predicates) {
        if (StringUtils.isEmpty(clusterName)) {
            return null;
        }

        Predicate ret = new Predicate() {
            @Override
            public boolean evaluate(Object object) {
                if (object == null) {
                    return false;
                }

                boolean ret = false;

                if (object instanceof RangerAuditMetrics) {
                    RangerAuditMetrics rangerAuditMetrics = (RangerAuditMetrics) object;
                    ret = StringUtils.equals(rangerAuditMetrics.getClusterName(), clusterName);
                } else if (object instanceof RangerAuditMetricsByHours) {
                    RangerAuditMetricsByHours rangerAuditMetricsByHours = (RangerAuditMetricsByHours) object;
                    ret = StringUtils.equals(rangerAuditMetricsByHours.getClusterName(), clusterName);
                } else if (object instanceof RangerAuditMetricsByDays) {
                    RangerAuditMetricsByDays rangerAuditMetricsByDays = (RangerAuditMetricsByDays) object;
                    ret = StringUtils.equals(rangerAuditMetricsByDays.getClusterName(), clusterName);
                }
                return ret;
            }
        };

        if (predicates != null) {
            predicates.add(ret);
        }

        return ret;
    }

    private Predicate addPredicateForClientIP(final String clientIP, List<Predicate> predicates) {
        if (StringUtils.isEmpty(clientIP)) {
            return null;
        }

        Predicate ret = new Predicate() {
            @Override
            public boolean evaluate(Object object) {
                if (object == null) {
                    return false;
                }

                boolean ret = false;

                if (object instanceof RangerAuditMetrics) {
                    RangerAuditMetrics rangerAuditMetrics = (RangerAuditMetrics) object;
                    ret = StringUtils.equals(rangerAuditMetrics.getclientIP(), clientIP);
                } else if (object instanceof RangerAuditMetricsByHours) {
                    RangerAuditMetricsByHours rangerAuditMetricsByHours = (RangerAuditMetricsByHours) object;
                    ret = StringUtils.equals(rangerAuditMetricsByHours.getClientIP(), clientIP);
                } else if (object instanceof RangerAuditMetricsByDays) {
                    RangerAuditMetricsByDays rangerAuditMetricsByDays = (RangerAuditMetricsByDays) object;
                    ret = StringUtils.equals(rangerAuditMetricsByDays.getClientIP(), clientIP);
                }
                return ret;
            }
        };

        if (predicates != null) {
            predicates.add(ret);
        }

        return ret;
    }
}
