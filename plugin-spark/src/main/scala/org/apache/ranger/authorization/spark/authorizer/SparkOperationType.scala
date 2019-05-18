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

/**
 * Subset of HiveOperationTypes supported by Apache Spark.
 *
 */
object SparkOperationType extends Enumeration {
  type SparkOperationType = Value

  val
  ALTERDATABASE, ALTERTABLE_ADDCOLS, ALTERTABLE_ADDPARTS, ALTERTABLE_RENAMECOL,
  ALTERTABLE_DROPPARTS, MSCK, ALTERTABLE_RENAMEPART, ALTERTABLE_RENAME,
  ALTERVIEW_RENAME, ALTERTABLE_PROPERTIES, ALTERTABLE_SERDEPROPERTIES,
  ALTERTABLE_LOCATION, QUERY, CREATEDATABASE, CREATETABLE_AS_SELECT, CREATEFUNCTION, CREATETABLE,
  CREATEVIEW, DESCTABLE, DESCDATABASE, DESCFUNCTION, DROPDATABASE, DROPTABLE, DROPFUNCTION, LOAD,
  SHOWCONF, SWITCHDATABASE, SHOW_CREATETABLE, SHOWCOLUMNS, SHOWDATABASES, SHOWFUNCTIONS,
  SHOWPARTITIONS, SHOWTABLES, SHOW_TBLPROPERTIES, TRUNCATETABLE, DROPVIEW, EXPLAIN = Value

}
