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

 package org.apache.ranger.service.filter;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.UriBuilder;
import org.apache.ranger.common.PropertiesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;


public class RangerRESTAPIFilter implements ContainerRequestFilter, ContainerResponseFilter {

	Logger logger = LoggerFactory.getLogger(RangerRESTAPIFilter.class);
	static volatile boolean initDone = false;

	boolean logStdOut = true;
	HashMap<String, String> regexPathMap = new HashMap<String, String>();
	HashMap<String, Pattern> regexPatternMap = new HashMap<String, Pattern>();
	List<String> regexList = new ArrayList<String>();
	List<String> loggedRestPathErrors = new ArrayList<String>();

	void init() {
		// double-checked locking
		if (!initDone) {
			synchronized (this) {
				if (!initDone) {
					logStdOut = PropertiesUtil.getBooleanProperty("xa.restapi.log.enabled", false);

					// Build hash map
					try {
						loadPathPatterns();
					} catch (Throwable t) {
						logger.error(
								"Error parsing REST classes for PATH patterns. Error ignored, but should be fixed immediately",
								t);
					}
					initDone = true;
				}
			}
		}
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see
	 * jakarta.ws.rs.container.ContainerRequestFilter#filter(jakarta.ws.rs.container.ContainerRequestContext)
	 */
	@Override
	public void filter(ContainerRequestContext request) {
		if (!initDone) {
			init();
		}
		if (logStdOut) {
			String path = request.getUriInfo().getPath();

			if ((request.getMediaType() == null || !"multipart".equals(request.getMediaType().getType()))
					&& !path.endsWith("/service/general/logs")) {
				try {
					logRequestDetails(request);
				} catch (Throwable t) {
					logger.error("Error FILTER logging. path=" + path, t);
				}
			}
		}
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see
	 * jakarta.ws.rs.container.ContainerResponseFilter#filter(
	 * jakarta.ws.rs.container.ContainerRequestContext,
	 * jakarta.ws.rs.container.ContainerResponseContext)
	 */
	@Override
	public void filter(ContainerRequestContext request,
			ContainerResponseContext response) {
		if (logStdOut) {
			// If it is image, then don't call super
			if (response.getMediaType() == null) {
				logger.info("DELETE ME: Response= mediaType is null");
			}
			if (response.getMediaType() == null
					|| !"image".equals(response.getMediaType().getType())) {
				logResponseDetails(response);
			}
		}
	}

	private void loadPathPatterns() throws ClassNotFoundException {
		String pkg = "org.apache.ranger.service";
		// List<Class> cList = findClasses(new File(dir), pkg);
		@SuppressWarnings("rawtypes")
		List<Class> cList = findClasses(pkg);
		for (@SuppressWarnings("rawtypes")
		Class klass : cList) {
			Annotation[] annotations = klass.getAnnotations();
			for (Annotation annotation : annotations) {
				if (!(annotation instanceof Path)) {
					continue;
				}
				Path path = (Path) annotation;
				if (path.value().startsWith("crud")) {
					continue;
				}
				// logger.info("path=" + path.value());
				// Loop over the class methods
				for (Method m : klass.getMethods()) {
					Annotation[] methodAnnotations = m.getAnnotations();
					String httpMethod = null;
					String servicePath = null;
					for (Annotation methodAnnotation : methodAnnotations) {
						if (methodAnnotation instanceof GET) {
							httpMethod = "GET";
						} else if (methodAnnotation instanceof PUT) {
							httpMethod = "PUT";
						} else if (methodAnnotation instanceof POST) {
							httpMethod = "POST";
						} else if (methodAnnotation instanceof DELETE) {
							httpMethod = "DELETE";
						} else if (methodAnnotation instanceof Path) {
							servicePath = ((Path) methodAnnotation)
									.value();
						}
					}

					if (httpMethod == null) {
						continue;
					}

					String fullPath = path.value();
					String regEx = httpMethod + ":" + path.value();

					if (servicePath != null) {
						if (!servicePath.startsWith("/")) {
							servicePath = "/" + servicePath;
						}

						UriBuilder uriBuilder = UriBuilder.fromPath(servicePath);
						regEx = httpMethod + ":" + path.value() + Pattern.quote(uriBuilder.build().getPath());
						fullPath += servicePath;
					}
					Pattern regexPattern = Pattern.compile(regEx);

					if (regexPatternMap.containsKey(regEx)) {
						logger.warn("Duplicate regex=" + regEx + ", fullPath="
								+ fullPath);
					}
					regexList.add(regEx);
					regexPathMap.put(regEx, fullPath);
					regexPatternMap.put(regEx, regexPattern);

					logger.info("path=" + path.value() + ", servicePath="
							+ servicePath + ", fullPath=" + fullPath
							+ ", regEx=" + regEx);
				}
			}
		}
		// ReOrder list
		int i = 0;
		for (i = 0; i < 10; i++) {
			boolean foundMatches = false;
			List<String> tmpList = new ArrayList<String>();
			for (int x = 0; x < regexList.size(); x++) {
				boolean foundMatch = false;
				String rX = regexList.get(x);
				for (int y = 0; y < x; y++) {
					String rY = regexList.get(y);
					Matcher matcher = regexPatternMap.get(rY).matcher(rX);
					if (matcher.matches()) {
						foundMatch = true;
						foundMatches = true;
						// logger.info("rX " + rX + " matched with rY=" + rY
						// + ". Moving rX to the top. Loop count=" + i);
						break;
					}
				}
				if (foundMatch) {
					tmpList.add(0, rX);
				} else {
					tmpList.add(rX);
				}
			}
			regexList = tmpList;
			if (!foundMatches) {
				logger.info("Done rearranging. loopCount=" + i);
				break;
			}
		}
		if (i == 10) {
			logger.warn("Couldn't rearrange even after " + i + " loops");
		}

		logger.info("Loaded " + regexList.size() + " API methods.");
		// for (String regEx : regexList) {
		// logger.info("regEx=" + regEx);
		// }
	}

	@SuppressWarnings("rawtypes")
	private List<Class> findClasses(String packageName)
			throws ClassNotFoundException {
		List<Class> classes = new ArrayList<Class>();

		ClassPathScanningCandidateComponentProvider scanner = new ClassPathScanningCandidateComponentProvider(
				true);

		// scanner.addIncludeFilter(new
		// AnnotationTypeFilter(<TYPE_YOUR_ANNOTATION_HERE>.class));

		for (BeanDefinition bd : scanner.findCandidateComponents(packageName)) {
			classes.add(Class.forName(bd.getBeanClassName()));
		}

		return classes;
	}

	private void logRequestDetails(ContainerRequestContext requestContext) {
		// Puedes registrar detalles de la petición, como el método y la URI
		String method = requestContext.getMethod();
		String uri = requestContext.getUriInfo().getRequestUri().toString();
		logger.info("Request - Method: {}, URI: {}", method, uri);
	}

	private void logResponseDetails(ContainerResponseContext responseContext) {
		// Puedes registrar detalles de la respuesta, como el código de estado y los headers
		int status = responseContext.getStatus();
		MediaType mediaType = responseContext.getMediaType();
		logger.info("Response - Status: {}, MediaType: {}", status, mediaType);
	}

}
