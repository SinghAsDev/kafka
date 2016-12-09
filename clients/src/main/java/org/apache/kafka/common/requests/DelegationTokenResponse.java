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
import org.apache.kafka.common.security.Token;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DelegationTokenResponse extends AbstractResponse {

    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentResponseSchema(ApiKeys.CREATE_DELEGATION_TOKEN.id);

    public static final String ERROR_CODE_KEY_NAME = "error_code";
    public static final String OWNER_KEY_NAME = "owner";
    public static final String RENEWERS_KEY_NAME = "renewers";
    public static final String CREATED_TIME_MS_KEY_NAME = "created_time_ms";
    public static final String EXPIRY_TIME_MS_KEY_NAME = "expiry_time_ms";
    public static final String MAX_LIFE_TIME_MS_KEY_NAME = "max_life_time_ms";
    public static final String TOKEN_ID_KEY_NAME = "token_id";
    public static final String HMAC_KEY_NAME = "hmac";

    /**
     * Possible error codes:
     *
     * TODO
     */
    private final short errorCode;
    private final KafkaPrincipal owner;
    private final Set<KafkaPrincipal> renewers;
    private final Long createdTimeMs;
    private final Long expiryTimeMs;
    private final Long maxLifeTimeMs;
    private final String tokenId;
    private final byte[] hmac;

    public DelegationTokenResponse(short errorCode, KafkaPrincipal owner,
                                   Set<KafkaPrincipal> renewers,
                                   Long createdTimeMs,
                                   Long expiryTimeMs,
                                   Long maxLifeTimeMs,
                                   String tokenId,
                                   byte[] hmac) {
        super(new Struct(CURRENT_SCHEMA));
        struct.set(ERROR_CODE_KEY_NAME, errorCode);
        struct.set(OWNER_KEY_NAME, owner.toString());
        List<String> renewersList = new ArrayList<>();
        for (KafkaPrincipal kafkaPrincipal : renewers) {
            renewersList.add(kafkaPrincipal.toString());
        }
        struct.set(RENEWERS_KEY_NAME, renewersList.toArray());
        struct.set(CREATED_TIME_MS_KEY_NAME, createdTimeMs);
        struct.set(EXPIRY_TIME_MS_KEY_NAME, expiryTimeMs);
        struct.set(MAX_LIFE_TIME_MS_KEY_NAME, maxLifeTimeMs);
        struct.set(TOKEN_ID_KEY_NAME, tokenId);
        struct.set(HMAC_KEY_NAME, ByteBuffer.wrap(hmac));

        this.errorCode = errorCode;
        this.owner = owner;
        this.renewers = renewers;
        this.createdTimeMs = expiryTimeMs;
        this.expiryTimeMs = expiryTimeMs;
        this.maxLifeTimeMs = maxLifeTimeMs;
        this.tokenId = tokenId;
        this.hmac = hmac;
    }

    public DelegationTokenResponse(Struct struct) {
        super(struct);
        this.errorCode = struct.getShort(ERROR_CODE_KEY_NAME);
        this.owner = KafkaPrincipal.fromString(struct.getString(OWNER_KEY_NAME));
        final Object[] renewersArray = struct.getArray(RENEWERS_KEY_NAME);
        List<KafkaPrincipal> renewersList = new ArrayList<>(renewersArray.length);
        for (Object renewerObj: renewersArray) {
            renewersList.add(KafkaPrincipal.fromString((String) renewerObj));
        }
        this.renewers = new HashSet<>(renewersList);
        this.createdTimeMs = struct.getLong(CREATED_TIME_MS_KEY_NAME);
        this.expiryTimeMs = struct.getLong(EXPIRY_TIME_MS_KEY_NAME);
        this.maxLifeTimeMs = struct.getLong(MAX_LIFE_TIME_MS_KEY_NAME);
        this.tokenId = struct.getString(TOKEN_ID_KEY_NAME);
        this.hmac = struct.getBytes(HMAC_KEY_NAME).array();
    }

    public Token token() {
        return new Token(owner, renewers, createdTimeMs, maxLifeTimeMs, expiryTimeMs, tokenId, hmac);
    }

    public short errorCode() {
        return errorCode;
    }

    public static DelegationTokenResponse parse(ByteBuffer buffer) {
        return new DelegationTokenResponse(CURRENT_SCHEMA.read(buffer));
    }

    public static DelegationTokenResponse fromError(Errors error) {
        return new DelegationTokenResponse(error.code(), KafkaPrincipal.ANONYMOUS, Collections.<KafkaPrincipal>emptySet(), -1L, -1L, -1L, "", "".getBytes());
    }
}