/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.admin

import java.util.Properties

import joptsimple._
import kafka.utils._
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.utils.Utils


object DelegationTokenCommand {

  val Newline = scala.util.Properties.lineSeparator

  def main(args: Array[String]) {

    val opts = new DelegationTokenCommandOptions(args)

    if (opts.options.has(opts.helpOpt))
      CommandLineUtils.printUsageAndDie(opts.parser, "Usage:")

    opts.checkArgs()

    val adminClient = {
      val props = if (opts.options.has(opts.commandConfigOpt)) Utils.loadProps(opts.options.valueOf(opts.commandConfigOpt)) else new Properties()
      props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, opts.options.valueOf(opts.bootstrapServerOpt))
      AdminClient.create(props)
    }

    try {
      if (opts.options.has(opts.createOpt))
        createDelegationToken(opts, adminClient)
      //else if (opts.options.has(opts.listOpt))
        //listDelegationToken(opts)
    } catch {
      case e: Throwable =>
        println(s"Error while executing Delegation Token command: ${e.getMessage}")
        println(Utils.stackTrace(e))
        System.exit(-1)
    }
  }

  private def createDelegationToken(opts: DelegationTokenCommandOptions, adminClient: AdminClient): Unit = {
    val renewers = if (opts.options.has(opts.renewersOpt)) opts.options.valueOf(opts.renewersOpt).split(",").toSet else Set.empty[String]
    val maxLifeTime = if (opts.options.has(opts.maxLifeTimeOpt)) opts.options.valueOf(opts.maxLifeTimeOpt) else -1L
    val token = adminClient.createDelegationToken(renewers, maxLifeTime)
    println(s"Created Delegation token: ${token.toString}")
  }


  private def confirmAction(opts: DelegationTokenCommandOptions, msg: String): Boolean = {
    if (opts.options.has(opts.forceOpt))
        return true
    println(msg)
    Console.readLine().equalsIgnoreCase("y")
  }

  class DelegationTokenCommandOptions(args: Array[String]) {
    val parser = new OptionParser

    val commandConfigOpt = parser.accepts("command-config", "Property file containing configs to be passed to Admin Client and Consumer.")
      .withRequiredArg
      .describedAs("command config property file")
      .ofType(classOf[String])

    val bootstrapServerOpt = parser.accepts("bootstrap-server", "REQUIRED (unless old consumer is used): The server to connect to.")
      .withRequiredArg
      .describedAs("server to connect to")
      .ofType(classOf[String])

    val createOpt = parser.accepts("create", "Indicates you are trying to create Delegation Token.")
    val removeOpt = parser.accepts("remove", "Indicates you are trying to remove Delegation Token.")
    val renewOpt = parser.accepts("renew", "Indicates you are trying to renew an existing Delegation Token.")
    val listOpt = parser.accepts("list", "List Delegation Tokens for the specified resource.")

    val renewersOpt = parser.accepts("renewers", "Users allowed to renew the created Delegation Token.")
      .withRequiredArg
      .describedAs("renewers")
      .ofType(classOf[String])

    val maxLifeTimeOpt = parser.accepts("max-life-time", "Maximum lifetime of the created Delegation Token in milliseconds.")
      .withRequiredArg
      .describedAs("max-life-time")
      .ofType(classOf[Long])

    val helpOpt = parser.accepts("help", "Print usage information.")

    val forceOpt = parser.accepts("force", "Assume Yes to all queries and do not prompt.")

    val options = parser.parse(args: _*)

    def checkArgs() {
      CommandLineUtils.checkRequiredArgs(parser, options)

      val actions = Seq(createOpt, removeOpt, renewOpt, listOpt).count(options.has)
      if (actions != 1)
        CommandLineUtils.printUsageAndDie(parser, "Command must include exactly one action: --create, --remove, --renew, --list. ")
    }
  }

}
