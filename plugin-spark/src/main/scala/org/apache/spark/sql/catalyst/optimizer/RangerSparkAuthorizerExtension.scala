/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 *   contributor license agreements.  See the NOTICE file distributed with
 *   this work for additional information regarding copyright ownership.
 *   The ASF licenses this file to You under the Apache License, Version 2.0
 *   (the "License"); you may not use this file except in compliance with
 *   the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.spark.sql.catalyst.optimizer

import org.apache.commons.logging.LogFactory
import org.apache.ranger.authorization.spark.authorizer.{RangerSparkAuthorizer, SparkAccessControlException, SparkOperationType}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.{CreateTempViewUsing, InsertIntoDataSourceCommand, InsertIntoHadoopFsRelationCommand}
import org.apache.spark.sql.execution.{RangerShowDatabasesCommand, RangerShowTablesCommand}
import org.apache.spark.sql.hive.PrivilegesBuilder
import org.apache.spark.sql.hive.execution.CreateHiveTableAsSelectCommand

/**
 * An Optimizer Rule to do Hive Authorization V2 for Spark SQL.
 *
 * For Apache Spark 2.2.x and later
 */
case class RangerSparkAuthorizerExtension(spark: SparkSession) extends Rule[LogicalPlan] {
  import SparkOperationType._

  private val LOG = LogFactory.getLog(classOf[RangerSparkAuthorizerExtension])

  /**
   * Visit the [[LogicalPlan]] recursively to get all spark privilege objects, check the privileges
   *
   * If the user is authorized, then the original plan will be returned; otherwise, interrupted by
   * some particular privilege exceptions.
   * @param plan a spark LogicalPlan for verifying privileges
   * @return a plan itself which has gone through the privilege check.
   */
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case s: ShowTablesCommand => RangerShowTablesCommand(s)
      case s: ShowDatabasesCommand => RangerShowDatabasesCommand(s)
      case r: RangerShowTablesCommand => r
      case r: RangerShowDatabasesCommand => r
      case _ =>
        val operationType: SparkOperationType = toOperationType(plan)
        val (in, out) = PrivilegesBuilder.build(plan)
        try {
          RangerSparkAuthorizer.checkPrivileges(spark, operationType, in, out)
          plan
        } catch {
          case ace: SparkAccessControlException =>
            LOG.error(
              s"""
                 |+===============================+
                 ||Spark SQL Authorization Failure|
                 ||-------------------------------|
                 ||${ace.getMessage}
                 ||-------------------------------|
                 ||Spark SQL Authorization Failure|
                 |+===============================+
               """.stripMargin)
            throw ace
        }
    }
  }

  /**
   * Mapping of [[LogicalPlan]] -> [[SparkOperationType]]
   * @param plan a spark LogicalPlan
   * @return
   */
  private def toOperationType(plan: LogicalPlan): SparkOperationType = {
    plan match {
      case c: Command => c match {
        case _: AlterDatabasePropertiesCommand => ALTERDATABASE
        case p if p.nodeName == "AlterTableAddColumnsCommand" => ALTERTABLE_ADDCOLS
        case _: AlterTableAddPartitionCommand => ALTERTABLE_ADDPARTS
        case p if p.nodeName == "AlterTableChangeColumnCommand" => ALTERTABLE_RENAMECOL
        case _: AlterTableDropPartitionCommand => ALTERTABLE_DROPPARTS
        case _: AlterTableRecoverPartitionsCommand => MSCK
        case _: AlterTableRenamePartitionCommand => ALTERTABLE_RENAMEPART
        case a: AlterTableRenameCommand => if (!a.isView) ALTERTABLE_RENAME else ALTERVIEW_RENAME
        case _: AlterTableSetPropertiesCommand
             | _: AlterTableUnsetPropertiesCommand => ALTERTABLE_PROPERTIES
        case _: AlterTableSerDePropertiesCommand => ALTERTABLE_SERDEPROPERTIES
        case _: AlterTableSetLocationCommand => ALTERTABLE_LOCATION
        case _: AlterViewAsCommand => QUERY

        case _: AnalyzeColumnCommand => QUERY
        // case _: AnalyzeTableCommand => HiveOperation.ANALYZE_TABLE
        // Hive treat AnalyzeTableCommand as QUERY, obey it.
        case _: AnalyzeTableCommand => QUERY
        case p if p.nodeName == "AnalyzePartitionCommand" => QUERY

        case _: CreateDatabaseCommand => CREATEDATABASE
        case _: CreateDataSourceTableAsSelectCommand
             | _: CreateHiveTableAsSelectCommand => CREATETABLE_AS_SELECT
        case _: CreateFunctionCommand => CREATEFUNCTION
        case _: CreateTableCommand
             | _: CreateDataSourceTableCommand => CREATETABLE
        case _: CreateTableLikeCommand => CREATETABLE
        case _: CreateViewCommand
             | _: CacheTableCommand
             | _: CreateTempViewUsing => CREATEVIEW

        case p if p.nodeName == "DescribeColumnCommand" => DESCTABLE
        case _: DescribeDatabaseCommand => DESCDATABASE
        case _: DescribeFunctionCommand => DESCFUNCTION
        case _: DescribeTableCommand => DESCTABLE

        case _: DropDatabaseCommand => DROPDATABASE
        // Hive don't check privileges for `drop function command`, what about a unverified user
        // try to drop functions.
        // We treat permanent functions as tables for verifying.
        case d: DropFunctionCommand if !d.isTemp => DROPTABLE
        case d: DropFunctionCommand if d.isTemp => DROPFUNCTION
        case _: DropTableCommand => DROPTABLE

        case e: ExplainCommand => toOperationType(e.logicalPlan)

        case _: InsertIntoDataSourceCommand => QUERY
        case p if p.nodeName == "InsertIntoDataSourceDirCommand" => QUERY
        case _: InsertIntoHadoopFsRelationCommand => CREATETABLE_AS_SELECT
        case p if p.nodeName == "InsertIntoHiveDirCommand" => QUERY
        case p if p.nodeName == "InsertIntoHiveTable" => QUERY

        case _: LoadDataCommand => LOAD

        case p if p.nodeName == "SaveIntoDataSourceCommand" => QUERY
        case s: SetCommand if s.kv.isEmpty || s.kv.get._2.isEmpty => SHOWCONF
        case _: SetDatabaseCommand => SWITCHDATABASE
        case _: ShowCreateTableCommand => SHOW_CREATETABLE
        case _: ShowColumnsCommand => SHOWCOLUMNS
        case _: ShowDatabasesCommand => SHOWDATABASES
        case _: ShowFunctionsCommand => SHOWFUNCTIONS
        case _: ShowPartitionsCommand => SHOWPARTITIONS
        case _: ShowTablesCommand => SHOWTABLES
        case _: ShowTablePropertiesCommand => SHOW_TBLPROPERTIES
        case s: StreamingExplainCommand =>
          toOperationType(s.queryExecution.optimizedPlan)

        case _: TruncateTableCommand => TRUNCATETABLE

        case _: UncacheTableCommand => DROPVIEW

        // Commands that do not need build privilege goes as explain type
        case _ =>
          // AddFileCommand
          // AddJarCommand
          // ...
          EXPLAIN
      }
      case _ => QUERY
    }
  }

}
