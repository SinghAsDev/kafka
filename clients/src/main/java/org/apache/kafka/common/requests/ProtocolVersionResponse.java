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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ProtocolVersionResponse extends AbstractRequestResponse {

    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentResponseSchema(ApiKeys.PROTOCOL_VERSION.id);

    public static final String ERROR_CODE_KEY_NAME = "error_code";
    public static final String PROTOCOL_VERSIONS_KEY_NAME = "protocol_versions";
    public static final String API_KEY_NAME = "api_key";
    public static final String API_NAME_KEY_NAME = "api_name";
    public static final String API_VERSIONS_KEY_NAME = "api_versions";
    public static final String API_DEPRECATED_VERSIONS_KEY_NAME = "api_deprecated_versions";

    /**
     * Possible error codes:
     * <p/>
     * AUTHORIZATION_FAILED (29)
     */

    private final short errorCode;

    public static final class ProtocolVersion {
        public final short apiKey;
        public final String apiName;
        public final List<Short> apiVersions;
        public final List<Short> apiDeprecatedVersions;

        public ProtocolVersion(short apiKey, String apiName, List<Short> apiVersions, List<Short> apiDeprecatedVersions) {
            this.apiKey = apiKey;
            this.apiName = apiName;
            this.apiVersions = apiVersions;
            this.apiDeprecatedVersions = apiDeprecatedVersions;
        }
    }

    private final List<ProtocolVersion> protocolVersions;

    public ProtocolVersionResponse(short errorCode, List<ProtocolVersion> protocolVersions) {
        super(new Struct(CURRENT_SCHEMA));
        struct.set(ERROR_CODE_KEY_NAME, errorCode);
        List<Struct> protocolVersionList = new ArrayList<>();
        for (ProtocolVersion protocolVersion : protocolVersions) {
            Struct protocolVersionStruct = struct.instance(PROTOCOL_VERSIONS_KEY_NAME);
            protocolVersionStruct.set(API_KEY_NAME, protocolVersion.apiKey);
            protocolVersionStruct.set(API_NAME_KEY_NAME, protocolVersion.apiName);
            protocolVersionStruct.set(API_VERSIONS_KEY_NAME, protocolVersion.apiVersions.toArray());
            protocolVersionStruct.set(API_DEPRECATED_VERSIONS_KEY_NAME, protocolVersion.apiDeprecatedVersions.toArray());
            protocolVersionList.add(protocolVersionStruct);
        }
        struct.set(PROTOCOL_VERSIONS_KEY_NAME, protocolVersionList.toArray());
        this.errorCode = errorCode;
        this.protocolVersions = protocolVersions;
    }

    public ProtocolVersionResponse(Struct struct) {
        super(struct);
        this.errorCode = struct.getShort(ERROR_CODE_KEY_NAME);
        this.protocolVersions = new ArrayList<>();
        for (Object protocolVersionsObj : struct.getArray(PROTOCOL_VERSIONS_KEY_NAME)) {
            Struct protocolVersionStruct = (Struct) protocolVersionsObj;
            short apiKey = protocolVersionStruct.getShort(API_KEY_NAME);
            String apiName = protocolVersionStruct.getString(API_NAME_KEY_NAME);
            List<Short> apiVersions = new ArrayList<>();
            for (Object apiVersion : protocolVersionStruct.getArray(API_VERSIONS_KEY_NAME)) {
                short apiVersionInt = (short) apiVersion;
                apiVersions.add(apiVersionInt);
            }
            List<Short> apiDeprecatedVersions = new ArrayList<>();
            for (Object apiDeprecatedVersion : protocolVersionStruct.getArray(API_DEPRECATED_VERSIONS_KEY_NAME)) {
                short apiDeprecatedVersionInt = (short) apiDeprecatedVersion;
                apiDeprecatedVersions.add(apiDeprecatedVersionInt);
            }
            this.protocolVersions.add(new ProtocolVersion(apiKey, apiName, apiVersions, apiDeprecatedVersions));
        }
    }

    public List<ProtocolVersion> protocolVersions() {
        return protocolVersions;
    }

    public short errorCode() {
        return errorCode;
    }

    public static ProtocolVersionResponse parse(ByteBuffer buffer) {
        return new ProtocolVersionResponse(CURRENT_SCHEMA.read(buffer));
    }

    public static ProtocolVersionResponse fromError(Errors error) {
        return new ProtocolVersionResponse(error.code(), Collections.<ProtocolVersion>emptyList());
    }

}
