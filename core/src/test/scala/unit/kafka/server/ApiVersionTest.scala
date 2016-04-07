/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package unit.kafka.server

import kafka.server.KafkaApis
import org.apache.kafka.common.requests.ApiVersionResponse.ApiVersion
import org.apache.kafka.common.protocol.{Protocol, ApiKeys}
import org.junit.Test

class ApiVersionTest {

  @Test
  def testBrokerAdvertiseToZK {
    val apiVersions = KafkaApis.apiVersions
    assert(apiVersions.size() == ApiKeys.values().size)

    for ((version, apiKey) <- apiVersions.toArray zip ApiKeys.values) {
      val ver: ApiVersion = version.asInstanceOf[ApiVersion]
      assert(ver.apiKey == apiKey.id)
      for (i <- 0 until ver.minVersion) {
        assert(Protocol.REQUESTS(ver.apiKey)(i) == null)
        assert(Protocol.RESPONSES(ver.apiKey)(i) == null)
      }
      for (i <- ver.minVersion.asInstanceOf[Int] to ver.maxVersion) {
        assert(Protocol.REQUESTS(ver.apiKey)(i) != null)
        assert(Protocol.RESPONSES(ver.apiKey)(i) != null)
      }
    }
  }
}