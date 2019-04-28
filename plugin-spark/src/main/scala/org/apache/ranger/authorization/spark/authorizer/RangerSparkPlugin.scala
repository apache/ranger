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

package org.apache.ranger.authorization.spark.authorizer

import java.io.{File, IOException}

import org.apache.commons.logging.LogFactory
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext.CLIENT_TYPE
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration
import org.apache.ranger.plugin.service.RangerBasePlugin

class RangerSparkPlugin private extends RangerBasePlugin("spark", "sparkSql") {
  import RangerSparkPlugin._

  private val LOG = LogFactory.getLog(classOf[RangerSparkPlugin])

  lazy val fsScheme: Array[String] = RangerConfiguration.getInstance()
    .get("ranger.plugin.spark.urlauth.filesystem.schemes", "hdfs:,file:")
    .split(",")
    .map(_.trim)

  override def init(): Unit = {
    super.init()
    val cacheDir = new File(rangerConf.get("ranger.plugin.spark.policy.cache.dir"))
    if (cacheDir.exists() &&
      (!cacheDir.isDirectory || !cacheDir.canRead || !cacheDir.canWrite)) {
      throw new IOException("Policy cache directory already exists at" +
        cacheDir.getAbsolutePath + ", but it is unavailable")
    }

    if (!cacheDir.exists() && !cacheDir.mkdirs()) {
      throw new IOException("Unable to create ranger policy cache directory at" +
        cacheDir.getAbsolutePath)
    }
    LOG.info("Policy cache directory successfully set to " + cacheDir.getAbsolutePath)
  }
}

object RangerSparkPlugin {

  private val rangerConf: RangerConfiguration = RangerConfiguration.getInstance

  val showColumnsOption: String = rangerConf.get(
    "xasecure.spark.describetable.showcolumns.authorization.option", "NONE")

  def build(): Builder = new Builder

  class Builder {

    @volatile private var sparkPlugin: RangerSparkPlugin = _

    def getOrCreate(): RangerSparkPlugin = RangerSparkPlugin.synchronized {
      if (sparkPlugin == null) {
        sparkPlugin = new RangerSparkPlugin
        sparkPlugin.init()
        sparkPlugin
      } else {
        sparkPlugin
      }
    }
  }
}
