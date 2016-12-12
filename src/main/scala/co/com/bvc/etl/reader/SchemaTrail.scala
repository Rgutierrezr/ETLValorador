package co.com.bvc.etl.reader

/**
  * Created by Milton_Lamprea on 06/12/2016.
  */

import org.apache.spark.sql.types.StructType

trait SchemaTrail {
  def getSchema(): StructType
}

