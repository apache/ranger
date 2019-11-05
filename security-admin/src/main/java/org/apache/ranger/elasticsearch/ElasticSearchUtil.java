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

package org.apache.ranger.elasticsearch;

import org.apache.log4j.Logger;
import org.apache.ranger.common.*;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

@Component
public class ElasticSearchUtil {
	private static final Logger logger = Logger.getLogger(ElasticSearchUtil.class);

	@Autowired
	RESTErrorUtil restErrorUtil;

	@Autowired
	StringUtil stringUtil;

	SimpleDateFormat dateFormat = new SimpleDateFormat(
			"yyyy-MM-dd'T'HH:mm:ss'Z'");

	public ElasticSearchUtil() {
		String timeZone = PropertiesUtil.getProperty("xa.solr.timezone");
		if (timeZone != null) {
			logger.info("Setting timezone to " + timeZone);
			try {
				dateFormat.setTimeZone(TimeZone.getTimeZone(timeZone));
			} catch (Throwable t) {
                                logger.error("Error setting timezone. TimeZone = " + timeZone);
			}
		}
	}


	// Utility methods
	public int toInt(Object value) {
		if (value == null) {
			return 0;
		}
		if (value instanceof Integer) {
			return (Integer) value;
		}
		if (value.toString().isEmpty()) {
			return 0;
		}
		try {
			return Integer.valueOf(value.toString());
		} catch (Throwable t) {
                        logger.error("Error converting value to integer. Value = " + value, t);
		}
		return 0;
	}

	public long toLong(Object value) {
		if (value == null) {
			return 0;
		}
		if (value instanceof Long) {
			return (Long) value;
		}
		if (value.toString().isEmpty()) {
			return 0;
		}
		try {
			return Long.valueOf(value.toString());
		} catch (Throwable t) {
                        logger.error("Error converting value to long. Value = " + value, t);
		}
		return 0;
	}

	public Date toDate(Object value) {
		if (value == null) {
			return null;
		}
		if (value instanceof Date) {
			return (Date) value;
		}
		try {
			// TODO: Do proper parsing based on Solr response value
			return new Date(value.toString());
		} catch (Throwable t) {
                        logger.error("Error converting value to date. Value = " + value, t);
		}
		return null;
	}

    public SearchResponse searchResources(SearchCriteria searchCriteria, List<SearchField> searchFields, List<SortField> sortFields, RestHighLevelClient client) throws IOException {
		SearchRequest searchRequest = new SearchRequest();
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.query(QueryBuilders.matchAllQuery());
		searchRequest.source(searchSourceBuilder);
		return client.search(searchRequest, RequestOptions.DEFAULT);
    }
}
