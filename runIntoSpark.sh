./bin/spark-submit \
  --class co.com.bvc.etl.reader.Spark_POC.scala \
  --master local[2]\
  C:\Users\milton_lamprea\Documents\IdeaProjects\KafkaClient\target\scala-2.11\kafkaclient_2.11-1.0.jar
  
spark-submit  --class co.com.bvc.etl.reader.Spark_POC.scala --master local[2] target\scala-2.11\kafkaclient_2.11-1.0.jar
  
  
	
	
	
	spark-submit  --class co.com.bvc.DerivadosOTC	--master local[2] derivadosotc_2.11-1.0.jar