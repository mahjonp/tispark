/*
 * Copyright 2017 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tispark

import com.pingcap.tikv.TiSession
import com.pingcap.tikv.exception.TiClientInternalException
import com.pingcap.tikv.meta.{TiDAGRequest, TiTableInfo, TiTimestamp}
import com.pingcap.tispark.utils.TiUtil
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.tispark.{TiHandleRDD, TiRDD}
import org.apache.spark.sql.types.StructType

case class TiDBRelation(session: TiSession,
                        tableRef: TiTableReference,
                        meta: MetaManager,
                        var ts: TiTimestamp = null)(
  @transient val sqlContext: SQLContext
) extends BaseRelation {
  val table: TiTableInfo = meta
    .getTable(tableRef.databaseName, tableRef.tableName)
    .getOrElse(throw new TiClientInternalException("Table not exist"))

  override lazy val schema: StructType = TiUtil.getSchemaFromTable(table)

  override def sizeInBytes: Long = tableRef.sizeInBytes

  def logicalPlanToRDD(dagRequest: TiDAGRequest): TiRDD =
    new TiRDD(dagRequest, session.getConf, tableRef, session, sqlContext.sparkSession)

  def dagRequestToRegionTaskExec(dagRequest: TiDAGRequest, output: Seq[Attribute]): SparkPlan = {
    val timestamp = dagRequest.getStartTs

    val tiHandleRDD =
      new TiHandleRDD(
        dagRequest,
        session.getConf,
        tableRef,
        timestamp,
        session,
        sqlContext.sparkSession
      )
    val handlePlan = HandleRDDExec(tiHandleRDD)
    // collect handles as a list
    val aggFunc = CollectHandles(handlePlan.attributeRef.last)

    val aggExpr = AggregateExpression(
      aggFunc,
      Complete,
      isDistinct = true,
      NamedExpression.newExprId
    )

    val sortAgg = TiUtil
      .planAggregateWithoutPartial(
        Seq(handlePlan.attributeRef.head), // group by region id
        Seq(aggExpr),
        Seq(handlePlan.output.head, aggExpr.resultAttribute), // output <region, handleList>
        handlePlan
      )
      .children
      .head

    RegionTaskExec(
      sortAgg,
      output,
      dagRequest,
      session.getConf,
      session.getTimestamp,
      session,
      sqlContext.sparkSession
    )
  }

  override def equals(obj: Any): Boolean = obj match {
    case other: TiDBRelation =>
      this.table.equals(other.table)
    case _ =>
      false
  }
}
