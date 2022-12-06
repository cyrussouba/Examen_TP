package suppression

import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import java.io.{FileNotFoundException, IOException}


object serviceSuppression extends App {

  println("Entrer votre identifiant pour la suppression")
  val idClavier = scala.io.StdIn.readLine()


  val configurationfile = "src/main/scala/configuration.json"
  val clientdatafile = "src/main/scala/Cyrus-service1.csv"
  val clientDataTempDir = "src/main/scala/tmp"
  val spark = SparkSession.builder().master("local").getOrCreate()
  import spark.implicits._
  case class Client (identifiantClient:String, nom: String, prenom: String, adresse: String, dateDeSouscription:String )

  def deleteAClientRow(data: Dataset[Client], idClient: String): Dataset[Client] = {
    data.filter(row => row.identifiantClient != idClient)
  }


  try {
    // Lecture fichier de configuration JSON
    val dfJSON = spark.read.option("multiline","true").json(configurationfile).cache()
    dfJSON.show(false)
    val clientsData = spark.read
      .format("csv")
      .option("header", "true")
      .option("sep", ";")
      .csv(clientdatafile)
      .as[Client]
    clientsData.show()

    // Test suppression
    val dataAfterDeletion = deleteAClientRow(clientsData, idClavier)
    dataAfterDeletion.show()

    // Test persistence dans fichier CSV
    val columns = Seq("identifiantClient", "Nom", "prenom", "Adresse", "DateDeSouscription")
    val dataframeAfterDeletion = dataAfterDeletion.toDF(columns:_*)
    dataframeAfterDeletion
      .repartition(1)
      .write
      .option("header","true")
      .option("delimiter",";")
      .mode(SaveMode.Overwrite)
      .csv(clientDataTempDir)

    /*
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    val srcPath = new Path("src/main/scala/tmp") // src/main/scala
    val destPath = new Path("src/main/scala")
    val srcFile = FileUtil.listFiles(new File("c:/tmp/address"))
      .filter(f => f.getPath.endsWith(".csv"))(0)
    //Copy the CSV file outside of Directory and rename
    FileUtil.copy(srcFile, hdfs, destPath, true, hadoopConfig)
    //Remove Directory created by df.write()
    hdfs.delete(srcPath, true)
    //Removes CRC File
    //hdfs.delete(new Path("/tmp/.address_merged.csv.crc"), true)

     */
  } catch {
    case e: FileNotFoundException => println("imposssible.")
    case ex: IOException => println("Had an IOException trying to read that file")
  }


}
