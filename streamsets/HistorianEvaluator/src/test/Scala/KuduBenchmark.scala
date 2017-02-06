import java.util.logging.{Level, Logger}

import com.acl.bigtablegen.DBManager
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.kudu.spark.kudu._
import org.apache.kudu.client.CreateTableOptions



class KuduDBManager(url: String) extends DBManager {

  /* address of Kudu master server (the URL, with the "kudu:" part removed) */
  val kuduMaster = url.substring("kudu://".length)

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("Spark SQL basic example")
    .getOrCreate()
  val sqlContext = spark.sqlContext

  /* get a reference to the Kudu master server, for operations */
  val kuduContext = new KuduContext(s"${kuduMaster}:7051")

  /**
    *
    * @param tableName
    * @param columnTypes
    */
  override def createTable(tableName: String, columnTypes: List[Char]) = {


    val structFields = columnTypes.zipWithIndex.map {
      typeWithIndex => {
        val colType = typeWithIndex._1
        val index = typeWithIndex._2

        colType match {
          case 's' => StructField("id", IntegerType, false)
          case 'i' => StructField(s"integer${index}", IntegerType)
          case 'f' => StructField(s"float${index}", FloatType)
          case '$' => StructField(s"currency${index}", StringType)
          case 'n' => StructField(s"name${index}", StringType)
          case 'c' => StructField(s"company${index}", StringType)
          case 'e' => StructField(s"email${index}", StringType)
          case 'p' => StructField(s"paragraph${index}", StringType)
          case 'h' => StructField(s"phone${index}", StringType)
          case '1' => StructField(s"group${index}", IntegerType)
          case 'd' => StructField(s"date${index}", DateType)
        }
      }
    }

    val schema = StructType(structFields)

    /* list of table keys to use for partitioning. Must be a Java list */
    val partitionKeyList = new java.util.ArrayList[String]
    partitionKeyList.add("id")

    /* create the table */
    kuduContext.createTable(
      tableName,
      schema,
      Seq("id"),
      new CreateTableOptions().setNumReplicas(1).addHashPartitions(partitionKeyList, 3)
    )

    /* to be used for uploading data */
    new KuduDBFormatter(tableName, this, columnTypes, schema, kuduContext)
  }

  def executeCommand(sql: String) = {

  }

  /**
    * Close the database connection
    */
  def close() = {
    /* nothing to do */
  }
}

/**
  * Created by peter_smith on 2016-11-16.
  */
class KuduDBFormatter(tableName: String,
                      dbManager: KuduDBManager,
                      columnTypes: List[Char],
                      tableSchema: StructType,
                      kuduContext: KuduContext) extends Formatter {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("Big table generator")
    .getOrCreate()

  var rowCount: Integer = _
  var totalRowCount: Integer = _
  var rowList: java.util.List[Row] = _


  val UploadBatchSize = 50000

  def startOutput() = {
    rowCount = 0
    totalRowCount = 0
  }

  def outputRow(row: List[String]) = {
    if (rowCount == 0) {
      rowList = new util.LinkedList[Row]()
    }
    outputOneRow(row)

    rowCount += 1
    if (rowCount == UploadBatchSize) {
      flush()
    }
  }

  def outputOneRow(row: List[String]) = {

    val fields = row.zip(columnTypes).map {
      fieldAndType =>
      {
        val field = fieldAndType._1
        val colType = fieldAndType._2

        colType match {
          case '$' | 'n' | 'c' | 'e' | 'p' | 'h' | 'd' => "\"" + field + "\""
          case 'f' => field.toFloat
          case _ => field.toInt
        }
      }
    }

    rowList.add(Row.fromSeq(fields))
  }

  def finishOutput() = {
    flush()
  }

  private def flush() = {

    /* flush may be called at any time, so only perform the flush if there's work to be done */
    if (rowCount != 0) {
      val dfToInsert = spark.createDataFrame(rowList, tableSchema)
      kuduContext.insertRows(dfToInsert, tableName)

      /* update counters */
      totalRowCount += rowCount
      rowCount = 0
      rowList.clear()
    }

    print(s"Rows so far: ${totalRowCount}\u000d")
  }
}
