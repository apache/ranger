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

package org.apache.ranger.plugin.model.validation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;

import com.google.common.collect.Lists;

public class RangerServiceDefHelper {

	private static final Log LOG = LogFactory.getLog(RangerServiceDefHelper.class);
	
	static final Map<String, Delegate> _Cache = new ConcurrentHashMap<String, Delegate>();
	final Delegate _delegate;

	public RangerServiceDefHelper(RangerServiceDef serviceDef) {
		this(serviceDef, true);
	}
	
	/**
	 * Intended for use when serviceDef object is not-trusted, e.g. when service-def is being created or updated. 
	 * @param serviceDef
	 * @param useCache
	 */
	public RangerServiceDefHelper(RangerServiceDef serviceDef, boolean useCache) {
		// NOTE: we assume serviceDef, its name and update time are can never by null.
		
		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("==> RangerPolicyValidator.isValidResourceNames(%s)", serviceDef));
		}

		String serviceName = serviceDef.getName();
		Date serviceDefFreshnessDate = serviceDef.getUpdateTime();

		Delegate delegate = null;
		if (useCache && _Cache.containsKey(serviceName)) {
			LOG.debug("RangerServiceDefHelper(): found delegate in cache with matching serviceName.  Need to check date");
			Delegate that = _Cache.get(serviceName);
			if (Objects.equals(that.getServiceFreshnessDate(), serviceDefFreshnessDate)) {
				delegate = that;
				LOG.debug("RangerServiceDefHelper(): cached delegate matched in date, too! Will use it now.");
			} else {
				LOG.debug("RangerServiceDefHelper(): cached delegate date mismatch!");
			}
		}
		if (delegate == null) { // either not found in cache or date didn't match
			delegate = new Delegate(serviceDef);
			if (useCache) {
				LOG.debug("RangerServiceDefHelper(): Created new delegate and put in delegate cache!");
				_Cache.put(serviceName, delegate);
			}
		}
		_delegate = delegate;
	}
	
	/**
	 * for a resource definition as follows:
	 * 
	 *  /-> E -> F
	 * A -> B -> C -> D
	 *       \-> G -> H
	 *        
	 * It would return a set with following ordered entries in it
	 * { [A B C D], [A E F], [A B G H] }
	 *  
	 * @return
	 */
	public Set<List<RangerResourceDef>> getResourceHierarchies(Integer policyType) {
		return _delegate.getResourceHierarchies(policyType);
	}

	public Set<String> getMandatoryResourceNames(List<RangerResourceDef> hierarchy) {
		Set<String> result = new HashSet<String>(hierarchy.size());
		for (RangerResourceDef resourceDef : hierarchy) {
			if (Boolean.TRUE.equals(resourceDef.getMandatory())) {
				result.add(resourceDef.getName());
			}
		}
		return result;
	}
	
	/**
	 * Set view of a hierarchy's resource names for efficient searching
	 * @param hierarchy
	 * @return
	 */
	public Set<String> getAllResourceNames(List<RangerResourceDef> hierarchy) {
		Set<String> result = new HashSet<String>(hierarchy.size());
		for (RangerResourceDef resourceDef : hierarchy) {
			result.add(resourceDef.getName());
		}
		return result;
	}
	
	/**
	 * Resources names matching the order of list of resource defs passed in.
	 * @param hierarchy
	 * @return
	 */
	public List<String> getAllResourceNamesOrdered(List<RangerResourceDef> hierarchy) {
		List<String> result = new ArrayList<String>(hierarchy.size());
		for (RangerResourceDef resourceDef : hierarchy) {
			result.add(resourceDef.getName());
		}
		return result;
	}

	public boolean isResourceGraphValid() {
		return _delegate.isResourceGraphValid();
	}

	/**
	 * Not designed for public access.  Package level only for testability.
	 */
	static class Delegate {

		final Map<Integer, Set<List<RangerResourceDef>>> _hierarchies = new HashMap<Integer, Set<List<RangerResourceDef>>>();
		final Date _serviceDefFreshnessDate;
		final String _serviceName;
		final boolean _valid;
		final static Set<List<RangerResourceDef>> EMPTY_RESOURCE_HIERARCHY = Collections.unmodifiableSet(new HashSet<List<RangerResourceDef>>());


		public Delegate(RangerServiceDef serviceDef) {
			// NOTE: we assume serviceDef, its name and update time are can never by null.
			_serviceName = serviceDef.getName();
			_serviceDefFreshnessDate = serviceDef.getUpdateTime();

			boolean isValid = true;
			for(Integer policyType : RangerPolicy.POLICY_TYPES) {
				List<RangerResourceDef> resources = getResourceDefs(serviceDef, policyType);
				DirectedGraph graph = createGraph(resources);

				if(graph != null) {
					if (isValid(graph)) {
						Set<List<String>> hierarchies = getHierarchies(graph);
						_hierarchies.put(policyType, Collections.unmodifiableSet(convertHierarchies(hierarchies, getResourcesAsMap(resources))));
					} else {
						isValid = false;
						_hierarchies.put(policyType, EMPTY_RESOURCE_HIERARCHY);
					}
				} else {
					_hierarchies.put(policyType, EMPTY_RESOURCE_HIERARCHY);
				}
			}
			_valid = isValid;
			if (LOG.isDebugEnabled()) {
				String message = String.format("Found [%d] resource hierarchies for service [%s] update-date[%s]: %s", _hierarchies.size(), _serviceName, 
						_serviceDefFreshnessDate == null ? null : _serviceDefFreshnessDate.toString(), _hierarchies); 
				LOG.debug(message);
			}
		}
		
		public Set<List<RangerResourceDef>> getResourceHierarchies(Integer policyType) {
			if(policyType == null) {
				policyType = RangerPolicy.POLICY_TYPE_ACCESS;
			}

			Set<List<RangerResourceDef>> ret = _hierarchies.get(policyType);

			if(ret == null) {
				ret = EMPTY_RESOURCE_HIERARCHY;
			}

			return ret;
		}

		public String getServiceName() {
			return _serviceName;
		}
		
		public Date getServiceFreshnessDate() {
			return _serviceDefFreshnessDate;
		}
		
		public boolean isResourceGraphValid() {
			return _valid;
		}
		/**
		 * Builds a directed graph where each resource is node and arc goes from parent level to child level
		 * 
		 * @param resourceDefs
		 * @return
		 */
		DirectedGraph createGraph(List<RangerResourceDef> resourceDefs) {
			DirectedGraph graph = null;

			if(CollectionUtils.isNotEmpty(resourceDefs)) {
				graph = new DirectedGraph();

				for (RangerResourceDef resourceDef : resourceDefs) {
					String name = resourceDef.getName();

					graph.add(name);
					String parent = resourceDef.getParent();
					if (StringUtils.isNotEmpty(parent)) {
						graph.addArc(parent, name);
					}
				}
			}

			if (LOG.isDebugEnabled()) {
				LOG.debug("Created graph for resources: " + graph);
			}
			return graph;
		}

		List<RangerResourceDef> getResourceDefs(RangerServiceDef serviceDef, Integer policyType) {
			final List<RangerResourceDef> resourceDefs;

			if(policyType == null || policyType == RangerPolicy.POLICY_TYPE_ACCESS) {
				resourceDefs = serviceDef.getResources();
			} else if(policyType == RangerPolicy.POLICY_TYPE_DATAMASK) {
				if(serviceDef.getDataMaskDef() != null) {
					resourceDefs = serviceDef.getDataMaskDef().getResources();
				} else {
					resourceDefs = null;
				}
			} else if(policyType == RangerPolicy.POLICY_TYPE_ROWFILTER) {
				if(serviceDef.getRowFilterDef() != null) {
					resourceDefs = serviceDef.getRowFilterDef().getResources();
				} else {
					resourceDefs = null;
				}
			} else { // unknown policyType; use all resources
				resourceDefs = serviceDef.getResources();
			}

			return resourceDefs;
		}
		/**
		 * A valid resource graph is a forest, i.e. a disjoint union of trees.  In our case, given that node can have only one "parent" node, we can detect this validity simply by ensuring that 
		 * the resource graph has:
		 * - at least one sink AND
		 * - and least one source.
		 * 
		 * A more direct method would have been ensure that the resulting graph does not have any cycles. 
		 * 
		 * @param graph
		 * 
		 * @return
		 */
		boolean isValid(DirectedGraph graph) {
			if (graph.getSources().size() > 0 && graph.getSinks().size() > 0) {
				return true;
			} else {
				return false;
			}
		}

		/**
		 * Returns all valid resource hierarchies for the configured resource-defs. Behavior is undefined if it is called on and invalid graph. Use <code>isValid</code> to check validation first.
		 * 
		 * @param graph
		 * @return
		 */
		Set<List<String>> getHierarchies(DirectedGraph graph) {
			Set<List<String>> hierarchies = new HashSet<List<String>>();
			Set<String> sources = graph.getSources();
			Set<String> sinks = graph.getSinks();
			for (String source : sources) {
				/*
				 * A disconnected node, i.e. one that does not have any arc coming into or out of it is a hierarchy in itself!
				 * A source by definition does not have any arcs coming into it.  So if it also doesn't have any neighbors then we know
				 * it is a disconnected node.
				 */
				if (!graph.hasNeighbors(source)) {
					List<String> path = Lists.newArrayList(source);
					hierarchies.add(path);
				} else {
					for (String sink : sinks) {
						List<String> path = graph.getAPath(source, sink, new HashSet<String>());
						if (!path.isEmpty()) {
							hierarchies.add(path);
						}
					}
				}
			}
			return hierarchies;
		}
		
		Set<List<RangerResourceDef>> convertHierarchies(Set<List<String>> hierarchies, Map<String, RangerResourceDef> resourceMap) {
			Set<List<RangerResourceDef>> result = new HashSet<List<RangerResourceDef>>(hierarchies.size());
			for (List<String> hierarchy : hierarchies) {
				List<RangerResourceDef> resourceList = new ArrayList<RangerResourceDef>(hierarchy.size());
				for (String name : hierarchy) {
					RangerResourceDef def = resourceMap.get(name);
					resourceList.add(def);
				}
				result.add(resourceList);
			}
			return result;
		}

		/**
		 * Converts resource list to resource map for efficient querying
		 * 
		 * @param resourceList
		 * @return
		 */
		Map<String, RangerResourceDef> getResourcesAsMap(List<RangerResourceDef> resourceList) {
			Map<String, RangerResourceDef> map = new HashMap<String, RangerResourceDef>(resourceList.size());
			for (RangerResourceDef resourceDef : resourceList) {
				map.put(resourceDef.getName(), resourceDef);
			}
			return map;
		}
	}

	/**
	 * Limited DAG implementation to analyze resource graph for a service. Not designed for public access.  Package level only for testability.
	 */
	static class DirectedGraph {
		Map<String, Set<String>> _nodes = new HashMap<String, Set<String>>();

		/**
		 * Add a node to the graph
		 * 
		 * @param node
		 */
		void add(String node) {
			if (node == null) {
				throw new IllegalArgumentException("Node can't be null!");
			} else if (!_nodes.containsKey(node)) { // don't mess with a node's neighbors if it already exists in the graph
				_nodes.put(node, new HashSet<String>());
			}
		}

		/**
		 * Connects node "from" to node "to". Being a directed graph, after this call "to" will be in the list of neighbor's of "from". While the converse need not be true.
		 * 
		 * @param from
		 * @param to
		 */
		void addArc(String from, String to) {
			// connecting two nodes, implicitly adds nodes to the graph if they aren't already in it
			if (!_nodes.containsKey(from)) {
				add(from);
			}
			if (!_nodes.containsKey(to)) {
				add(to);
			}
			_nodes.get(from).add(to);
		}

		/**
		 * Returns true if "to" is in the list of neighbors of "from"
		 * 
		 * @param from
		 * @param to
		 * @return
		 */
		boolean hasArc(String from, String to) {
			if (_nodes.containsKey(from) && _nodes.containsKey(to) && _nodes.get(from).contains(to)) {
				return true;
			} else {
				return false;
			}
		}

		/**
		 * Returns true if the node "from" has any neighbor.
		 * @param from
		 * @return
		 */
		boolean hasNeighbors(String from) {
			return _nodes.containsKey(from) && _nodes.get(from).size() > 0;
		}
		/**
		 * Return the set of nodes with in degree of 0, i.e. those that are not in any other nodes' list of neighbors
		 * 
		 * @return
		 */
		Set<String> getSources() {
			Set<String> sources = new HashSet<String>(_nodes.keySet());
			for (Map.Entry<String, Set<String>> entry : _nodes.entrySet()) {
				Set<String> nbrs = entry.getValue(); // can never be null
				sources.removeAll(nbrs); // A source in a DAG can't be a neighbor of any other node
			}
			if (LOG.isDebugEnabled()) {
				LOG.debug("Returning sources: " + sources);
			}
			return sources;
		}

		/**
		 * Returns the set of nodes with out-degree of 0, i.e. those nodes whose list of neighbors is empty
		 * 
		 * @return
		 */
		Set<String> getSinks() {
			Set<String> sinks = new HashSet<String>();
			for (Map.Entry<String, Set<String>> entry : _nodes.entrySet()) {
				Set<String> nbrs = entry.getValue();
				if (nbrs.isEmpty()) { // A sink in a DAG doesn't have any neighbor
					String node = entry.getKey();
					sinks.add(node);
				}
			}
			if (LOG.isDebugEnabled()) {
				LOG.debug("Returning sinks: " + sinks);
			}
			return sinks;
		}

		/**
		 * Attempts to do a depth first traversal of a graph and returns the resulting path. Note that there could be several paths that connect node "from" to node "to".
		 * 
		 * @param from
		 * @param to
		 * @return
		 */
		List<String> getAPath(String from, String to, Set<String> alreadyVisited) {
			List<String> path = new ArrayList<String>(_nodes.size());
			if (_nodes.containsKey(from) && _nodes.containsKey(to)) { // one can never reach non-existent nodes
				if (hasArc(from, to)) {
					path.add(from);
					path.add(to);
				} else {
					alreadyVisited.add(from);
					Set<String> nbrs = _nodes.get(from);
					for (String nbr : nbrs) {
						if (!alreadyVisited.contains(nbr)) {
							List<String> subPath = getAPath(nbr, to, alreadyVisited);
							if (!subPath.isEmpty()) {
								path.add(from);
								path.addAll(subPath);
							}
						}
					}
				}
			}
			return path;
		}
		
		@Override
		public boolean equals(Object object) {
			if (object == this) {
				return true;
			}
			if (object == null || object.getClass() != this.getClass()) {
				return false;
			}
			DirectedGraph that = (DirectedGraph)object;
			return Objects.equals(this._nodes, that._nodes);
		}
		
		@Override
		public int hashCode() {
			return Objects.hashCode(_nodes);
		}
		
		@Override
		public String toString() {
			return "_nodes=" + Objects.toString(_nodes);
		}
	}
}
