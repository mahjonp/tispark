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

import com.pingcap.tikv._
import com.pingcap.tikv.codec.KeyUtils
import com.pingcap.tikv.key.{Key, RowKey}
import com.pingcap.tikv.meta.TiTimestamp
import com.pingcap.tikv.region.TiRegion
import com.pingcap.tikv.row.ObjectRowImpl
import com.pingcap.tikv.txn.TxnKVClient
import com.pingcap.tikv.util.{ConcreteBackOffer, KeyRangeUtils}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.TiContext
import org.slf4j.LoggerFactory

/**
  * An ugly implementation of batch write framework, which will be
  * replaced by spark api.
  */
object TiBatchWrite {
  private final val logger = LoggerFactory.getLogger(getClass.getName)

  type SparkRow = org.apache.spark.sql.Row
  type TiRow = com.pingcap.tikv.row.Row
  type TiDataType = com.pingcap.tikv.types.DataType

  def writeToTiDB(rdd: RDD[SparkRow], tableRef: TiTableReference, tiContext: TiContext) {
    // initialize
    val tiConf = tiContext.tiConf
    val kvClient = createTxnKVClient(tiConf)

    // TODO: region pre-split
    // pending: https://internal.pingcap.net/jira/browse/TISPARK-69

    // TODO: lock table
    // pending: https://internal.pingcap.net/jira/browse/TIDB-1628

    // shuffle data in same task which belong to same region
    val shuffledRDD = {
      val tiKVRowRDD = rdd.map(sparkRow2TiKVRow)
      shuffleToSameRegion(tiKVRowRDD, tableRef, tiContext)
    }

    // TODO: resolve lock
    // why here? can we do resolve after receiving an error during writing data?

    // take one row as primary key
    val (primaryKey: SerializableKey, primaryRow: TiRow) = {
      val takeOne = shuffledRDD.take(1)
      if (takeOne.length == 0) {
        logger.warn("there is no data in source rdd")
        return
      } else {
        takeOne(0)
      }
    }
    logger.info(s"primary key: $primaryKey primary row: $primaryRow")

    // filter primary key
    val finalWriteRDD = shuffledRDD.filter {
      case (key, row) => !key.equals(primaryKey)
    }

    // get timestamp as start_ts
    val startTS = kvClient.getTimestamp
    logger.info(s"startTS: $startTS")

    // driver primary pre-write
    val ti2PCClient = create2PCClient(kvClient, startTS)
    val prewritePrimaryBackoff = ConcreteBackOffer.newCustomBackOff(3000)
    ti2PCClient.prewritePrimaryKey(prewritePrimaryBackoff, primaryKey.bytes, encodeTiRow(primaryRow))

    // executors secondary pre-write
    finalWriteRDD.foreachPartition { case iterator =>
      val kvClientOnExecutor = createTxnKVClient(tiConf)
      val ti2PCClientOnExecutor = create2PCClient(kvClientOnExecutor, startTS)
      val prewriteSecondaryBackoff = ConcreteBackOffer.newCustomBackOff(3000)

      import scala.collection.JavaConverters._
      val keys = iterator.map { case (key, row) => new TiBatchWrite2PCClient.ByteWrapper(key.bytes) }.asJava

      val values = iterator.map { case (key, row) => new TiBatchWrite2PCClient.ByteWrapper(encodeTiRow(row)) }.asJava

      ti2PCClientOnExecutor.prewriteSecondaryKeys(prewriteSecondaryBackoff, keys, values)
    }

    // driver primary commit
    val commitPrimaryBackoff = ConcreteBackOffer.newCustomBackOff(3000)
    ti2PCClient.commitPrimaryKey(commitPrimaryBackoff, primaryKey.bytes)

    // executors secondary commit
    finalWriteRDD.foreachPartition { case iterator =>
      val kvClientOnExecutor = createTxnKVClient(tiConf)
      val ti2PCClientOnExecutor = create2PCClient(kvClientOnExecutor, startTS)
      val commitSecondaryBackoff = ConcreteBackOffer.newCustomBackOff(3000)

      import scala.collection.JavaConverters._
      val keys = iterator.map { case (key, row) => new TiBatchWrite2PCClient.ByteWrapper(key.bytes) }.asJava

      ti2PCClientOnExecutor.commitSecondaryKeys(commitSecondaryBackoff, keys)
    }

    // print for test
    finalWriteRDD.foreachPartition { iterator =>
      println("==partition begin==")
      iterator.foreach {
        case (key, row) =>
          println("key=" + key.toString + " row=" + row.toString)
      }
    }

  }

  private def create2PCClient(kVClient: TxnKVClient, startTS: TiTimestamp): TiBatchWrite2PCClient = {
    new TiBatchWrite2PCClient(kVClient, startTS.getPhysical)
  }

  private def createTxnKVClient(tiConf: TiConfiguration): TxnKVClient = {
    val session = new TiSession(tiConf)
    val pdClient = PDClient.create(session)
    new TxnKVClient(tiConf, session.getRegionStoreClientBuilder, pdClient)
  }

  private def shuffleToSameRegion(rdd: RDD[TiRow],
                                  tableRef: TiTableReference,
                                  tiContext: TiContext): RDD[(SerializableKey, TiRow)] = {
    val regions = getRegions(tableRef, tiContext)
    val tiRegionPartitioner = new TiRegionPartitioner(regions)
    val databaseName = tableRef.databaseName
    val tableName = tableRef.tableName
    val session = tiContext.tiSession
    val table = session.getCatalog.getTable(databaseName, tableName)
    val tableId = table.getId

    logger.info(
      s"find ${regions.size} regions in database: $databaseName table: $tableName tableId: $tableId"
    )

    rdd
      .map(row => (tiKVRow2Key(row, tableId), row))
      .groupByKey(tiRegionPartitioner)
      .map {
        case (key, iterable) =>
          // remove duplicate rows if key equals
          (key, iterable.head)
      }
  }

  private def getRegions(tableRef: TiTableReference, tiContext: TiContext): List[TiRegion] = {
    import scala.collection.JavaConversions._
    TiBatchWriteUtils
      .getRegionsByTable(tiContext.tiSession, tableRef.databaseName, tableRef.tableName)
      .toList
  }

  private def tiKVRow2Key(row: TiRow, tableId: Long): SerializableKey = {
    // TODO: how to get primary key if exists || auto generate a primary key if does not exists
    // pending: https://internal.pingcap.net/jira/browse/TISPARK-70
    val handle = row.getLong(0)

    val rowKey = RowKey.toRowKey(tableId, handle)
    new SerializableKey(rowKey.getBytes)
  }

  private def sparkRow2TiKVRow(sparkRow: SparkRow): TiRow = {
    val fieldCount = sparkRow.size
    val tiRow = ObjectRowImpl.create(fieldCount)
    for (i <- 0 until fieldCount) {
      val data = sparkRow.get(i)
      val sparkDataType = sparkRow.schema(i).dataType
      val tiDataType = TiUtils.fromSparkType(sparkDataType)
      tiRow.set(i, tiDataType, data)
    }
    tiRow
  }

  private def encodeTiRow(tiRow: TiRow): Array[Byte] = {
    // TODO
    null
  }
}

class TiRegionPartitioner(regions: List[TiRegion]) extends Partitioner {
  override def numPartitions: Int = regions.length

  override def getPartition(key: Any): Int = {
    val serializableKey = key.asInstanceOf[SerializableKey]
    val rawKey = Key.toRawKey(serializableKey.bytes)

    regions.indices.foreach { i =>
      val region = regions(i)
      val range = KeyRangeUtils.makeRange(region.getStartKey, region.getEndKey)

      if (range.contains(rawKey)) {
        return i
      }
    }

    0
  }
}

class SerializableKey(val bytes: Array[Byte]) extends Serializable {
  override def toString: String = KeyUtils.formatBytes(bytes)

  override def equals(that: Any): Boolean = {
    that match {
      case that: SerializableKey => this.bytes.sameElements(that.bytes)
      case _ => false
    }
  }
}
