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

package org.apache.ranger.authorization.kafka.authorizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.classloader.RangerPluginClassLoader;

import scala.collection.immutable.Set;
import kafka.network.RequestChannel.Session;
import kafka.security.auth.Acl;
import kafka.security.auth.Authorizer;
import kafka.security.auth.KafkaPrincipal;
import kafka.security.auth.Operation;
import kafka.security.auth.Resource;
import kafka.server.KafkaConfig;


//public class RangerKafkaAuthorizer extends Authorizer {
public class RangerKafkaAuthorizer implements Authorizer {
	private static final Log LOG  = LogFactory.getLog(RangerKafkaAuthorizer.class);

	private static final String   RANGER_PLUGIN_TYPE                      = "kafka";
	private static final String[] RANGER_PLUGIN_LIB_DIR                   = new String[] {"lib/ranger-kafka-plugin"};
	private static final String   RANGER_KAFKA_AUTHORIZER_IMPL_CLASSNAME  = "org.apache.ranger.authorization.kafka.authorizer.RangerKafkaAuthorizer";

	private Authorizer  rangerKakfaAuthorizerImpl 						  = null;
	private static		RangerPluginClassLoader rangerPluginClassLoader   = null;
	
	public RangerKafkaAuthorizer() {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerKafkaAuthorizer.RangerKafkaAuthorizer()");
		}

		this.init();

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerKafkaAuthorizer.RangerKafkaAuthorizer()");
		}
	}
	
	private void init(){
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerKafkaAuthorizer.init()");
		}

		try {
			
			rangerPluginClassLoader = RangerPluginClassLoader.getInstance(RANGER_PLUGIN_TYPE, this.getClass());
			
			@SuppressWarnings("unchecked")
			Class<Authorizer> cls = (Class<Authorizer>) Class.forName(RANGER_KAFKA_AUTHORIZER_IMPL_CLASSNAME, true, rangerPluginClassLoader);

			activatePluginClassLoader();

			rangerKakfaAuthorizerImpl = cls.newInstance();
		} catch (Exception e) {
			// check what need to be done
			LOG.error("Error Enabling RangerKafkaPluing", e);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerKafkaAuthorizer.init()");
		}
	}
	
	
	@Override
	public void initialize(KafkaConfig kafkaConfig) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerKafkaAuthorizer.initialize()");
		}

		try {
			activatePluginClassLoader();

			rangerKakfaAuthorizerImpl.initialize(kafkaConfig);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerKafkaAuthorizer.initialize()");
		}
	}

	@Override
	public boolean authorize(Session session, Operation operation,Resource resource) {	
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerKafkaAuthorizer.authorize()");
		}

		boolean ret = false;
		
		try {
			activatePluginClassLoader();

			ret = rangerKakfaAuthorizerImpl.authorize(session, operation, resource);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerKafkaAuthorizer.authorize()");
		}
		
		return ret;
	}

	@Override
	public void addAcls(Set<Acl> acls, Resource resource) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerKafkaAuthorizer.addAcls()");
		}

		try {
			activatePluginClassLoader();

			rangerKakfaAuthorizerImpl.addAcls(acls, resource);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerKafkaAuthorizer.addAcls()");
		}
	}

	@Override
	public boolean removeAcls(Set<Acl> acls, Resource resource) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerKafkaAuthorizer.removeAcls()");
		}
		boolean ret = false;
		try {
			activatePluginClassLoader();

			ret = rangerKakfaAuthorizerImpl.removeAcls(acls, resource);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerKafkaAuthorizer.removeAcls()");
		}
		
		return ret;
	}

	@Override
	public boolean removeAcls(Resource resource) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerKafkaAuthorizer.removeAcls()");
		}
		boolean ret = false;
		try {
			activatePluginClassLoader();

			ret = rangerKakfaAuthorizerImpl.removeAcls(resource);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerKafkaAuthorizer.removeAcls()");
		}

		return ret;
	}

	@Override
	public Set<Acl> getAcls(Resource resource) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerKafkaAuthorizer.getAcls()");
		}
		
		Set<Acl> ret = null;
		
		try {
			activatePluginClassLoader();

			ret = rangerKakfaAuthorizerImpl.getAcls(resource);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerKafkaAuthorizer.getAcls()");
		}

		return ret;
	}

	@Override
	public Set<Acl> getAcls(KafkaPrincipal principal) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerKafkaAuthorizer.getAcls()");
		}

		Set<Acl> ret = null;

		try {
			activatePluginClassLoader();

			ret = rangerKakfaAuthorizerImpl.getAcls(principal);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerKafkaAuthorizer.getAcls()");
		}

		return ret;
	}
	
	private void activatePluginClassLoader() {
		if(rangerPluginClassLoader != null) {
			rangerPluginClassLoader.activate();
		}
	}

	private void deactivatePluginClassLoader() {
		if(rangerPluginClassLoader != null) {
			rangerPluginClassLoader.deactivate();
		}
	}
		
}