package co.com.bvc.etl.reader

import java.io.FileInputStream
import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Properties

import co.com.bvc.kafka.KafkaProducer
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import play.api.libs.json.JsArray
import play.api.libs.json.Json

import scala.io.Source


object ETLValorador {

  val logger = org.apache.log4j.LogManager.getLogger("ETLValorador")

  case class Contract(seccode: String, maturityDate: String, underId: String, status: String)

  def retrieveInputs(sc: SparkContext, sqlContext: SQLContext, prop: Properties): Unit = {

    var JsonStringSend = ""
    var JsonSupStringSend = ""
    var JsonSupVStringSend = ""
    var JsonCanStringSend = ""
    val fechaActual = new java.sql.Date(new java.util.Date().getTime)
    val topic = prop.getProperty("topic")
    val key = prop.getProperty("key")
    val inputsFolder = prop.getProperty("inputs.folder")

    logger.info("retrieving data: Contracts")
    retrieveContracts
    logger.info("retrieving data: IBR Daily")
    retrieveIBRDaily

    def retrieveContracts() = {
      val url = prop.getProperty("nasdaq.url")
      val username = prop.getProperty("nasdaq.user")
      val password = prop.getProperty("nasdaq.password")
      val query = prop.getProperty("nasdaq.qContratos")
      var connection: Connection = null
      try {
        Class.forName(prop.getProperty("nasdaq.driver"))
        connection = DriverManager.getConnection(url, username, password)
        val statement = connection.createStatement()
        val resultSet = statement.executeQuery(query)
        val df = resultSetToDataframe(resultSet, sqlContext)

        logger.info("data frame created for contracts")

        val jsonContracts = df.toJSON
        val arrayCanastaLength = jsonContracts.collect().length.toString()
        val insumoCanastaHeader = df.columns.toList
        val jsonTransCanasta = transformarJsonDF(df, "5", arrayCanastaLength, insumoCanastaHeader)

        logger.info("Canasta:\n"+jsonTransCanasta)

        connection.close()
      } catch {
        case e: Exception => logger.error(e.getMessage)
      }

      def resultSetToDataframe(resultSet: ResultSet, sqlContext: SQLContext): DataFrame = {
        var seqContratosE = Seq(Contract("", "", "", ""))
        while (resultSet.next()) {
          seqContratosE = seqContratosE ++ Seq(Contract(resultSet.getString(1).trim, resultSet.getString(2).trim, resultSet.getString(3).trim, resultSet.getString(4).trim))
        }
        val seqContratosEI = seqContratosE
        val rdd = sc.parallelize(seqContratosE)
        import sqlContext.implicits._
        rdd.toDF
      }
    }

    def retrieveIBRDaily = {
      /** ********* IBR DIARIA ************/
      val ibrDiaria = sqlContext.read
        .format("com.databricks.spark.csv")
        .option("header", "false")
        .option("inferSchema", "false")
        .load(s"${prop.getProperty("inputs.folder.local")}/${prop.getProperty("inputs.filename.ibrdiaria")}")
        .cache()
      val jsonIBR = ibrDiaria.toJSON
      val arrayIBRLength = jsonIBR.collect().length.toString()
      val insumoIBRHeader = ibrDiaria.columns.toList
      val jsonTransIBR = transformarJsonDF(ibrDiaria, "2", arrayIBRLength, insumoIBRHeader)
      logger.info("IBR:\n"+jsonTransIBR)
      KafkaProducer.sendMessage(topic, key, "Message")
    }

    def transformarJsonDF(arrayInsumo: DataFrame, idInsumo: String, lengthArray: String, listHeader: List[String]): String = {
      var jsonArray = new JsArray()
      var contador = 0;
      val arrayHeader = searchIdPropiedades(idInsumo)
      for (objIBR <- arrayInsumo) {
        contador = contador + 1
        for (i <- 0 to (listHeader.length - 1)) {
          val objIBRJson = Json.obj(
            "propiedad_insumo_propiedad_id" -> (arrayHeader(i)).toString(),
            "propiedad_insumo_insumo_id" -> idInsumo,
            "propiedad_insumo_fecha" -> fechaActual.toString(),
            "fila_id" -> contador,
            "value" -> objIBR.get(i).toString().trim())
          jsonArray = jsonArray.:+(objIBRJson)
        }

        if (contador.toString().equals(lengthArray)) {
          JsonStringSend = jsonArray.toString()
        }
      }
      JsonStringSend

    }


    def searchIdPropiedades(idInsumo: String): Array[String] = {
      var arrayIdPropiedades = Array("0")
      val prop = new Properties()
      prop.load(Source.fromFile("propiedades-ins.properties").bufferedReader())
      //prop.load(new FileInputStream("propiedades-ins.properties")) // Java

      //ID PROPIEDADES IBR
      if (idInsumo.equals("2")) {
        val arrayProp = prop.getProperty("array.props.ibr")
        val arrayIdProp: Array[String] = arrayProp.split(",")
        //var arrayIdProp = Array("26", "32")
        arrayIdPropiedades = arrayIdProp
      }

      //ID PROPIEDADES IBR LIBOR
      if (idInsumo.equals("3")) {
        val arrayProp = prop.getProperty("array.props.ibrl")
        val arrayIdProp: Array[String] = arrayProp.split(",")
        //var arrayIdProp = Array("26", "32")
        arrayIdPropiedades = arrayIdProp
      }

      //ID PROPIEDADES CURVA FORDWARD
      if (idInsumo.equals("18")) {
        val arrayProp = prop.getProperty("array.props.curvaf")
        val arrayIdProp: Array[String] = arrayProp.split(",")
        //var arrayIdProp = Array("26", "32")
        arrayIdPropiedades = arrayIdProp
      }

      //ID PROPIEDADES SUPERFICIE USD
      if (idInsumo.equals("4")) {
        val arrayProp = prop.getProperty("array.props.superficie")
        val arrayIdProp: Array[String] = arrayProp.split(",")
        //var arrayIdProp = Array("26", "27", "28", "29", "30", "31")
        arrayIdPropiedades = arrayIdProp
      }

      //ID PROPIEDADES SUPERFICIE VOLATILIDAD ECO CALL
      if (idInsumo.equals("10")) {
        val arrayProp = prop.getProperty("array.props.superficie.vec")
        val arrayIdProp: Array[String] = arrayProp.split(",")
        //var arrayIdProp = Array("83", "82", "81", "80", "79", "78", "77")
        arrayIdPropiedades = arrayIdProp
      }
      //ID PROPIEDADES SUPERFICIE VOLATILIDAD ECO PUT
      if (idInsumo.equals("11")) {
        val arrayProp = prop.getProperty("array.props.superficie.vep")
        val arrayIdProp: Array[String] = arrayProp.split(",")
        //var arrayIdProp = Array("83", "82", "81", "80", "79", "78", "77")
        arrayIdPropiedades = arrayIdProp
      }
      //ID PROPIEDADES SUPERFICIE VOLATILIDAD PFA CALL
      if (idInsumo.equals("13")) {
        val arrayProp = prop.getProperty("array.props.superficie.vpfac")
        val arrayIdProp: Array[String] = arrayProp.split(",")
        //var arrayIdProp = Array("83", "82", "81", "80", "79", "78", "77")
        arrayIdPropiedades = arrayIdProp
      }
      //ID PROPIEDADES SUPERFICIE VOLATILIDAD PFA PUT
      if (idInsumo.equals("14")) {
        val arrayProp = prop.getProperty("array.props.superficie.vpfap")
        val arrayIdProp: Array[String] = arrayProp.split(",")
        //var arrayIdProp = Array("83", "82", "81", "80", "79", "78", "77")
        arrayIdPropiedades = arrayIdProp
      }
      //ID PROPIEDADES SUPERFICIE VOLATILIDAD PFB PUT
      if (idInsumo.equals("15")) {
        val arrayProp = prop.getProperty("array.props.superficie.vpfbp")
        val arrayIdProp: Array[String] = arrayProp.split(",")
        // var arrayIdProp = Array("83", "82", "81", "80", "79", "78", "77")
        arrayIdPropiedades = arrayIdProp
      }
      //ID PROPIEDADES SUPERFICIE VOLATILIDAD PFB CALL
      if (idInsumo.equals("16")) {
        val arrayProp = prop.getProperty("array.props.superficie.vpfbc")
        val arrayIdProp: Array[String] = arrayProp.split(",")
        //var arrayIdProp = Array("83", "82", "81", "80", "79", "78", "77")
        arrayIdPropiedades = arrayIdProp
      }

      //ID PROPIEDADES CANASTA
      if (idInsumo.equals("19")) {
        val arrayProp = prop.getProperty("array.props.canasta")
        val arrayIdProp: Array[String] = arrayProp.split(",")
        //var arrayIdProp = Array("01", "71", "70", "69", "68", "67", "66","65")
        arrayIdPropiedades = arrayIdProp
      }

      //ID PROPIEDADES CONTRATOS ACTIVOS
      if (idInsumo.equals("20")) {
        val arrayProp = prop.getProperty("array.props.contratos")
        val arrayIdProp: Array[String] = arrayProp.split(",")
        //var arrayIdProp NoClassDefFoundError= Array("01", "07", "73", "74")
        arrayIdPropiedades = arrayIdProp
      }

      //ID PROPIEDADES FESTIVO
      if (idInsumo.equals("21")) {
        val arrayProp = prop.getProperty("array.props.festivo")
        val arrayIdProp: Array[String] = arrayProp.split(",")
        //var arrayIdProp = Array("84")
        arrayIdPropiedades = arrayIdProp
      }


      if (idInsumo.equals("5")) {
        val arrayProp = prop.getProperty("array.props.contracts")
        val arrayIdProp: Array[String] = arrayProp.split(",")
        //var arrayIdProp = Array("26", "32")
        arrayIdPropiedades = arrayIdProp
      }
      arrayIdPropiedades

    }


  }

  def main(args: Array[String]): Unit = {
    logger.info("Running ETLValorador")
    val prop = new Properties()
    prop.load(new FileInputStream("etl.properties"))
    val appName = prop.getProperty("spark.appName")
    val master = prop.getProperty("spark.master")
    val conf = new SparkConf().setAppName(appName).setMaster(master)
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    ETLValorador.retrieveInputs(sc, sqlContext, prop)
  }
}