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

package com.xasecure.authorization.storm.authorizer;

import org.apache.ranger.authorization.storm.authorizer.RangerStormAuthorizer;

/**
 * This class exists only to provide for seamless upgrade/downgrade capabilities.  Class name is in storm conf which must allow for seamles upgrade and downgrade.
 * Thus when class names were changed XaSecure* -> Ranger* this shell class serves to allow for seamles upgrade as well as downgrade.
 * <p>
 * This class is final because if one needs to customize coprocessor it is expected that RangerStormAuthorizer would be modified/extended as that is
 * the "real" authorizer!  This class, hence, should NEVER be more than an EMPTY shell!
 */
public final class XaSecureStormAuthorizer extends RangerStormAuthorizer {
}
