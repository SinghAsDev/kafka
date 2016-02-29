/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package unit.kafka.cluster

import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Protocol
import org.apache.kafka.common.requests.ProtocolVersionResponse.ProtocolVersion
import org.junit.Test

class ProtocolVersionsTest extends ZooKeeperTestHarness {

  @Test
  def testGetProtocolVersions() {

    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(0, zkConnect, enableControlledShutdown = false))
    val server = TestUtils.createServer(config)
    val versions: java.util.List[ProtocolVersion] = server.apis.getProtocolVersions()
    assert(versions.size() == ApiKeys.values.size)
    versions.toArray.foreach( version => {
    })

    for ((version, apiKey) <- versions.toArray zip ApiKeys.values) {
      val ver: ProtocolVersion = version.asInstanceOf[ProtocolVersion]
      assert(ver.apiKey == apiKey.id)
      assert(ver.apiName == apiKey.name)
      assert(ver.apiVersions.size() == Protocol.REQUESTS(apiKey.id).count(x => x!= null))
      assert(ver.apiDeprecatedVersions.size() == 0)
    }
  }
}
