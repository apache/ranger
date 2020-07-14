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

package org.apache.ranger.authorization.elasticsearch.plugin.utils;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetRequest.Item;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.action.support.single.instance.InstanceShardOperationRequest;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.rest.RestRequest;

public class RequestUtils {
	public static final String CLIENT_IP_ADDRESS = "ClientIPAddress";

	public static String getClientIPAddress(RestRequest request) {
		SocketAddress socketAddress = request.getHttpChannel().getRemoteAddress();
		if (socketAddress instanceof InetSocketAddress) {
			return ((InetSocketAddress) socketAddress).getAddress().getHostAddress();
		}

		return null;
	}

	// To support all kinds of request in elasticsearch
	public static <Request extends ActionRequest> List<String> getIndexFromRequest(Request request) {
		List<String> indexs = new ArrayList<>();

		if (request instanceof SingleShardRequest) {
			indexs.add(((SingleShardRequest<?>) request).index());
			return indexs;
		}

		if (request instanceof ReplicationRequest) {
			indexs.add(((ReplicationRequest<?>) request).index());
			return indexs;
		}

		if (request instanceof InstanceShardOperationRequest) {
			indexs.add(((InstanceShardOperationRequest<?>) request).index());
			return indexs;
		}

		if (request instanceof CreateIndexRequest) {
			indexs.add(((CreateIndexRequest) request).index());
			return indexs;
		}

		if (request instanceof PutMappingRequest) {
			if (((PutMappingRequest) request).getConcreteIndex() != null) {
				indexs.add(((PutMappingRequest) request).getConcreteIndex().getName());
				return indexs;
			} else {
				return Arrays.asList(((PutMappingRequest) request).indices());
			}
		}

		if (request instanceof SearchRequest) {
			return Arrays.asList(((SearchRequest) request).indices());
		}

		if (request instanceof IndicesStatsRequest) {
			return Arrays.asList(((IndicesStatsRequest) request).indices());
		}

		if (request instanceof OpenIndexRequest) {
			return Arrays.asList(((OpenIndexRequest) request).indices());
		}

		if (request instanceof DeleteIndexRequest) {
			return Arrays.asList(((DeleteIndexRequest) request).indices());
		}

		if (request instanceof BulkRequest) {
			@SuppressWarnings("rawtypes") List<DocWriteRequest<?>> requests = ((BulkRequest) request).requests();

			if (CollectionUtils.isNotEmpty(requests)) {
				for (DocWriteRequest<?> docWriteRequest : requests) {
					indexs.add(docWriteRequest.index());
				}
				return indexs;
			}
		}

		if (request instanceof MultiGetRequest) {
			List<Item> items = ((MultiGetRequest) request).getItems();
			if (CollectionUtils.isNotEmpty(items)) {
				for (Item item : items) {
					indexs.add(item.index());
				}
				return indexs;
			}
		}

		// No matched request type to find specific index , set default value *
		indexs.add("*");
		return indexs;
	}
}
