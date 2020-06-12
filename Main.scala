import java.util.{Properties, UUID}
import java.util
import org.apache.flink.api.scala._
import com.bingocloud.{ClientConfiguration, Protocol}
import com.bingocloud.auth.BasicAWSCredentials
import com.bingocloud.services.s3.AmazonS3Client
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.nlpcn.commons.lang.util.IOUtil
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

object Main {
  //s3参数
  val accessKey = "3A2959DC8E8DB2AAC727"
  val secretKey = "W0FGRTU1NjI1OTU2ODE5MzI1QUJFNUU0M0I4Mzk0QTZDRkJEOTQwMkRd"
  val endpoint = "scuts3.depts.bingosoft.net:29999"
  val bucket = "qqs-big-bucket"
  //要读取的文件
  val key = "daas.txt"

  //kafka参数
  val topic = "Data_flow_of_qqs"
  val bootstrapServers = "bigdata35.depts.bingosoft.net:29035,bigdata36.depts.bingosoft.net:29036,bigdata37.depts.bingosoft.net:29037"
  val period=5000

  def main(args: Array[String]): Unit = {
    val s3Content = readFile()
    produceToKafka(s3Content)
    kafkaConsumer()
  }

  /**
   * 从s3中读取文件内容
   *
   * @return s3的文件内容
   */
  def readFile(): String = {
    val credentials = new BasicAWSCredentials(accessKey, secretKey)
    val clientConfig = new ClientConfiguration()
    clientConfig.setProtocol(Protocol.HTTP)
    val amazonS3 = new AmazonS3Client(credentials, clientConfig)
    amazonS3.setEndpoint(endpoint)
    val s3Object = amazonS3.getObject(bucket, key)
    IOUtil.getContent(s3Object.getObjectContent, "UTF-8")
  }

  /**
   * 把数据写入到kafka中
   *
   * @param s3Content 要写入的内容
   */
  def produceToKafka(s3Content: String): Unit = {
    val props = new Properties
    props.put("bootstrap.servers", bootstrapServers)
    props.put("acks", "all")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    val dataArr = s3Content.split("\n")
    for (s <- dataArr) {
      if (!s.trim.isEmpty) {
        val record = new ProducerRecord[String, String](topic, null, s)
//        println("开始生产数据：" + s)
        producer.send(record)
      }
    }
    producer.flush()
    producer.close()
  }


  def kafkaConsumer():Unit={
    val keyPrefix="upload2/"
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val prop=new Properties()
    prop.put("bootstrap.servers",bootstrapServers)
    prop.put("auto.offset.reset","earliest")
    prop.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    prop.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")
    prop.put("group.id",UUID.randomUUID().toString)
    val kafkaConsumer = new FlinkKafkaConsumer010[String](topic,
      new SimpleStringSchema, prop)
    kafkaConsumer.setCommitOffsetsOnCheckpoints(true)
    val inputStream = env.addSource(kafkaConsumer)
    inputStream.map(x=>(x,getValue(x,4))).keyBy(1).timeWindow(Time.seconds(20))
      .reduce((a,b)=>(new String(a._1+"\n"+b._1),a._2))
      .writeUsingOutputFormat(new S3Writer(accessKey,secretKey,endpoint,bucket,keyPrefix,period))
    env.execute()
  }

  def getRows(str:String):util.ArrayList[String]={
    val strLen=str.length
    //去除{}
    val newStr=str.substring(1,strLen-1)
    //分离
    val list=newStr.split(",")
    val result=new util.ArrayList[String]
    for(i<-0 until list.length){
      val subList=list(i).split(":")//subList(0)为key，subList(1)为value
      var valueStr=subList(1)
      if(valueStr.charAt(0)==' '){
        valueStr=valueStr.substring(1)
      }
      val vlen=valueStr.length
      val value=valueStr.substring(1,vlen-1)
      result.add(value)

    }
    return result
  }

  def getValue(str: String, i: Int):String={
    val x=getRows(str)
    return x.get(i)
  }

}