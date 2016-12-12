package co.com.bvc.etl.reader

/**
  * Created by Milton_Lamprea on 06/12/2016.
  */

import org.apache.spark.sql.types.{StructType, StructField, StringType}

object SchemaContract extends SchemaTrail {
  def getSchema(): StructType = {
    val schema = StructType(Array(
      StructField("SECCCODE", StringType),
      StructField("MATURITY_DATE", StringType),
      StructField("UNDER_ID", StringType),
      StructField("STATUS", StringType)
    ))
    schema
  }
}

