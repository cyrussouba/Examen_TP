package suppression

import com.google.common.hash.Hashing
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import java.io.{FileNotFoundException, IOException}
import java.nio.charset.StandardCharsets

object ServiceHashing extends App{
  println("Entrer votre identifiant pour le hashage")
  val idClavier = scala.io.StdIn.readLine()


  val configurationfile = "src/main/scala/configuration.json"
  val clientdatafile = "src/main/scala/Cyrus-service1.csv"
  val clientDataTempDir = "src/main/scala/tmp"
  val spark = SparkSession.builder().master("local").getOrCreate()
  import spark.implicits._

  case class Client(identifiantClient: String, nom: String, prenom: String, adresse: String, dateDeSouscription: String)

  def hashFieldData(client: Client) = {
    // hash each field of the client
    Hashing.sha256().hashString(client.identifiantClient, StandardCharsets.UTF_8).toString()
    Hashing.sha256().hashString(client.nom, StandardCharsets.UTF_8).toString()
    Hashing.sha256().hashString(client.prenom, StandardCharsets.UTF_8).toString()
    Hashing.sha256().hashString(client.adresse, StandardCharsets.UTF_8).toString()
    Hashing.sha256().hashString(client.dateDeSouscription, StandardCharsets.UTF_8).toString()
    client
  }

  def hashAClientData(data: Dataset[Client], idClient: String): Unit = {
    data.filter(row => row.identifiantClient == idClient)
      .map(dataUnit => hashFieldData(dataUnit))
  }

  try {
    // Lecture fichier de configuration JSON
    val dfJSON = spark.read.option("multiline", "true").json(configurationfile).cache()
    dfJSON.show(false)
    val clientsData = spark.read
      .format("csv")
      .option("header", "true")
      .option("sep", ";")
      .csv(clientdatafile)
      .as[Client]
    clientsData.show()

    // Test hashing
    hashAClientData(clientsData, idClavier)
    // Clients data after hashing
    clientsData.show()

    // Test persistence dans fichier CSV
    val columns = Seq("identifiantClient", "Nom", "prenom", "Adresse", "DateDeSouscription")
    val dataframeAfterDeletion = clientsData.toDF(columns: _*)
    dataframeAfterDeletion
      .repartition(1)
      .write
      .option("header", "true")
      .option("delimiter", ";")
      .mode(SaveMode.Overwrite)
      .csv(clientDataTempDir)

  } catch {
    case e: FileNotFoundException => println("imposssible.")
    case ex: IOException => println("Had an IOException trying to read that file")
  }
}
