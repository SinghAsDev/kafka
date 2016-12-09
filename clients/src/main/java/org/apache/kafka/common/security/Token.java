/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.security;

import org.apache.kafka.common.security.auth.KafkaPrincipal;

import java.util.Arrays;
import java.util.Set;

public class Token {

    private KafkaPrincipal owner;
    private Set<KafkaPrincipal> renewers;
    private Long createdTimeMs;
    private Long maxLifeTimeMs;
    private Long expiryTimeMs;
    private String id;
    private byte[] hmac;

    public Token(KafkaPrincipal owner, Set<KafkaPrincipal> renewers, Long createdTimeMs, Long maxLifeTimeMs, Long expiryTimeMs, String id, byte[] hmac) {
        if (owner == null || id == null || hmac == null) {
            throw new IllegalArgumentException("Owner or id or hmac can not be null");
        }
        this.owner = owner;
        this.renewers = renewers;
        this.createdTimeMs = createdTimeMs;
        this.maxLifeTimeMs = maxLifeTimeMs;
        this.expiryTimeMs = expiryTimeMs;
        this.id = id;
        this.hmac = hmac;
    }

    @Override
    public String toString() {
        return "Delegation Token {" +
                "  owner: " + owner +
                "  renewers: " + Arrays.toString(renewers.toArray()) +
                "  created time (ms): " + createdTimeMs +
                "  max lifetime (ms): " + maxLifeTimeMs +
                "  expiry time (ms): " + expiryTimeMs +
                "  id: " + id +
                "  hmac: " + hmac +
                "}";
    }
}