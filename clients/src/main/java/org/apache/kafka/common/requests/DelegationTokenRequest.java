/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.requests;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DelegationTokenRequest extends AbstractRequest {

    public static final String RENEWERS_KEY_NAME = "renewers";
    public static final String MAX_LIFE_TIME_KEY_NAME = "max_life_time";

    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentRequestSchema(ApiKeys.CREATE_DELEGATION_TOKEN.id);

    private final Set<KafkaPrincipal> renewers;
    private final Long maxLifeTime;


    public DelegationTokenRequest(Set<KafkaPrincipal> renewers, Long maxLifeTime) {
        super(new Struct(CURRENT_SCHEMA));
        this.renewers = renewers;
        this.maxLifeTime = maxLifeTime;

        List<String> renewersStrings = new ArrayList<>(renewers.size());
        for (KafkaPrincipal renewer: renewers) {
            renewersStrings.add(renewer.toString());
        }

        struct.set(RENEWERS_KEY_NAME, renewersStrings.toArray());
        struct.set(MAX_LIFE_TIME_KEY_NAME, maxLifeTime);
    }

    public DelegationTokenRequest(Struct struct) {
        super(struct);
        Object[] renewersArray = struct.getArray(RENEWERS_KEY_NAME);
        renewers = new HashSet<>(renewersArray.length);
        for (Object renewer : renewersArray)
            renewers.add(KafkaPrincipal.fromString((String) renewer));
        this.maxLifeTime = struct.getLong(MAX_LIFE_TIME_KEY_NAME);
    }

    @Override
    public AbstractResponse getErrorResponse(int versionId, Throwable e) {
        switch (versionId) {
            case 0:
                return DelegationTokenResponse.fromError(Errors.forException(e));
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ProtoUtils.latestVersion(ApiKeys.CREATE_DELEGATION_TOKEN.id)));
        }
    }

    public static DelegationTokenRequest parse(ByteBuffer buffer, int versionId) {
        return new DelegationTokenRequest(ProtoUtils.parseRequest(ApiKeys.CREATE_DELEGATION_TOKEN.id, versionId, buffer));
    }

    public static DelegationTokenRequest parse(ByteBuffer buffer) {
        return new DelegationTokenRequest(CURRENT_SCHEMA.read(buffer));
    }

    public Long maxLifeTime() {
        return maxLifeTime;
    }

    public Set<KafkaPrincipal> renewers() {
        return renewers;
    }
}