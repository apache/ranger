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

package org.apache.hadoop.crypto.key.common;

import javax.crypto.spec.PBEKeySpec;

import java.util.function.BiFunction;

public enum KeySpecStrategy {
    @Deprecated
    PASSWORD_ONLY((password, algoParamSpec) -> new PBEKeySpec(password)),

    PASSWORD_SALT_ITERATIONCOUNT((password, algoParamSpec) -> new PBEKeySpec(password, algoParamSpec.getSalt(), algoParamSpec.getIterationCount())),

    PASSWORD_SALT_ITERATIONCOUNT_KEYLENGTH((password, algoParamSpec) -> new PBEKeySpec(password, algoParamSpec.getSalt(), algoParamSpec.getIterationCount(), algoParamSpec.getKeyLength().get()));

    private BiFunction<char[], RangerCipherSuite.AlgoParamSpec, PBEKeySpec> pbeKeySpecBiFunction;

    KeySpecStrategy(BiFunction<char[], RangerCipherSuite.AlgoParamSpec, PBEKeySpec> pbeKeySpecBiFunction) {
        this.pbeKeySpecBiFunction = pbeKeySpecBiFunction;
    }

    public PBEKeySpec createPBEKeySpec(char[] password, RangerCipherSuite.AlgoParamSpec algoParamSpec) {
        return pbeKeySpecBiFunction.apply(password, algoParamSpec);
    }
}
