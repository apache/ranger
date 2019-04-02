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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.security.UserGroupInformation
import org.apache.ranger.authorization.spark.authorizer._
import org.apache.ranger.plugin.policyengine.RangerAccessResult
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.AuthzUtils.getFieldVal
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation

import scala.collection.JavaConverters._

case class RangerSparkRowFilterExtension(spark: SparkSession) extends Rule[LogicalPlan] {

  private lazy val sparkPlugin = RangerSparkPlugin.build().getOrCreate()

  /**
    * Transform a Relation to a parsed [[LogicalPlan]] with specified row filter expressions
    * @param plan the original [[LogicalPlan]]
    * @param table a Spark [[CatalogTable]] representation
    * @return A new Spark [[LogicalPlan]] with specified row filter expressions
    */
  private def applyingRowFilterExpr(plan: LogicalPlan, table: CatalogTable): LogicalPlan = {
    val auditHandler = new RangerSparkAuditHandler()
    try {
      val identifier = table.identifier
      val resource =
        RangerSparkResource(SparkObjectType.TABLE, identifier.database, identifier.table)
      val ugi = UserGroupInformation.getCurrentUser
      val request = new RangerSparkAccessRequest(resource, ugi.getShortUserName,
        ugi.getGroupNames.toSet.asJava, SparkObjectType.TABLE.toString, SparkAccessType.SELECT,
        null, null, sparkPlugin.getClusterName)
      val result = sparkPlugin.evalRowFilterPolicies(request, auditHandler)
      if (isRowFilterEnabled(result)) {
        val sql = s"select ${plan.output.map(_.name).mkString(",")} from ${table.qualifiedName}" +
          s" where ${result.getFilterExpr}"
        spark.sessionState.sqlParser.parsePlan(sql)
      } else {
        plan
      }
    } catch {
      case e: Exception => throw e
    }
  }

  private def isRowFilterEnabled(result: RangerAccessResult): Boolean = {
    result != null && result.isRowFilterEnabled && StringUtils.isNotEmpty(result.getFilterExpr)
  }

  /**
    * Transform a spark logical plan to another plan with the row filer expressions
    * @param plan the original [[LogicalPlan]]
    * @return the logical plan with row filer expressions applied
    */
  override def apply(plan: LogicalPlan): LogicalPlan = {
    var newPlan = plan
    newPlan = plan transform {
      case h if h.nodeName == "HiveTableRelation" =>
        val ct = getFieldVal(h, "tableMeta").asInstanceOf[CatalogTable]
        applyingRowFilterExpr(h, ct)
      case m if m.nodeName == "MetastoreRelation" =>
        val ct = getFieldVal(m, "catalogTable").asInstanceOf[CatalogTable]
        applyingRowFilterExpr(m, ct)
      case l: LogicalRelation if l.catalogTable.isDefined =>
        applyingRowFilterExpr(l, l.catalogTable.get)
    }
    Dataset.ofRows(spark, newPlan).queryExecution.analyzed
  }
}
