import ch.qos.logback.classic.ClassicConstants
import Main.logger
import com.google.gson.Gson
import models.{PPZBMedXml_Parameters, ZBMedpp_doc}
import org.slf4j.{Logger, LoggerFactory}
import ppzbmedxml.ZBMedppToXml
import mongodb.MongoExport
import org.mongodb.scala.Document

import java.util.Date
import scala.util.{Failure, Success, Try}

class Main {

  private def exportXml(parameters: PPZBMedXml_Parameters): Try[Unit] = {
    Try {
      System.out.println("\n")
      logger.info(s"Migration started - ZBMed preprints ${new Date()}")

      val zbmedpp = new ZBMedppToXml
      val mExportRead: MongoExport = new MongoExport(parameters.databaseRead, parameters.collectionRead, parameters.hostRead, parameters.portRead)
      val mExportWrite: MongoExport = new MongoExport(parameters.databaseWrite.getOrElse(parameters.databaseRead),
        parameters.collectionWrite.getOrElse(parameters.collectionRead), parameters.hostWrite, parameters.portWrite)
      val docsMongo: Seq[Document] = mExportRead.findAll

      existDocumentsOrStop(docsMongo, parameters)

      zbmedpp.toXml(docsMongo, parameters.xmlOut) match {
        case Success(value) =>
          logger.info(s"Writing normalized documents in collection: ${parameters.collectionWrite}")
          value.zipWithIndex.foreach{
            case (f, index) =>
              insertDocumentNormalized(f, mExportWrite, parameters.collectionWrite.get)
              zbmedpp.amountProcessed(value.length, index + 1, if (value.length >= 10000) 10000 else value.length)
              if (index == value.length) logger.info(s"FILE GENERATED SUCCESSFULLY IN: ${parameters.xmlOut}")
          }
          s"\n${logger.info(s"FILE GENERATED SUCCESSFULLY IN: ${parameters.xmlOut}")}"
        case Failure(_) => logger.warn("FAILURE TO GENERATE FILE")
      }
    }
  }


  def insertDocumentNormalized(doc: ZBMedpp_doc, mExport: MongoExport, nameCollectionNormalized: String): Unit = {

    val docJson: String = new Gson().toJson(doc)

    if (!mExport.existCollection(nameCollectionNormalized)) {
      mExport.createCollection(nameCollectionNormalized)
    }
    val isFieldRepeted = mExport.isIdRepetedNormalized("id", doc.id)
    if (!isFieldRepeted) {
      mExport.insertDocumentNormalized(docJson)
    }
  }

  def existDocumentsOrStop(docsMongo: Seq[Document], parameters: PPZBMedXml_Parameters): Unit ={

    docsMongo.length match {
      case docs if docs == 0 => throw new Exception(s"${logger.warn("No documents found check collection and parameters")}")
      case docs if docs > 0 => logger.info(s"Connected to mongodb - database: ${parameters.databaseRead}, collection: ${parameters.collectionRead}," +
        s" host: ${parameters.hostRead.getOrElse("localhost")}, port: ${parameters.portRead.getOrElse(27017)}, user: ${parameters.userRead.getOrElse("None")}")
        logger.info(s"Total documents: ${docsMongo.length}")
    }
  }
}

object Main {

  System.setProperty(ClassicConstants.CONFIG_FILE_PROPERTY, "./src/main/scala/resources/logback.xml")
  val logger: Logger = LoggerFactory.getLogger(classOf[Main])


  private def usage(): Unit = {
    System.err.println("-xmlout=<path>            - XML file output directory")
    System.err.println("-databaseRead=<name>      - MongoDB database name")
    System.err.println("-collectionRead=<name>    - MongoDB database collection name")
    System.err.println("[-hostRead=<name>]        - MongoDB server name. Default value is 'localhost'")
    System.err.println("[-portRead=<number>]      - MongoDB server port number. Default value is 27017")
    System.err.println("[-userRead=<name>])       - MongoDB user name")
    System.err.println("[-passwordRead=<pwd>]     - MongoDB user password")
    System.err.println("[-databaseWrite=<name>]   - MongoDB database name Normalized")
    System.err.println("[-collectionWrite=<name>] - MongoDB database collection name Normalized")
    System.err.println("[-hostWrite=<name>]       - MongoDB server name. Default value is 'localhost' Normalized")
    System.err.println("[-portWrite=<number>]     - MongoDB server port number. Default value is 27017 Normalized")
    System.err.println("[-userWrite=<name>])      - MongoDB user name Normalized")
    System.err.println("[-passwordWrite=<pwd>]    - MongoDB user password Normalized")
    System.exit(1)
  }

  def main(args: Array[String]): Unit = {

    if (args.length < 3) usage()

    val parameters: Map[String, String] = args.foldLeft[Map[String, String]](Map()) {
      case (map, par) =>
        val split = par.split(" *= *", 2)
        if (split.size == 1) map + ((split(0).substring(2), ""))
        else map + (split(0).substring(1) -> split(1))
    }

    if (!Set("xmlout", "databaseRead", "collectionRead").forall(parameters.contains)) usage()

    val xmlOut: String = parameters("xmlout")
    val databaseRead: String = parameters("databaseRead")
    val collectionRead: String = parameters("collectionRead")
    val hostRead: Option[String] = parameters.get("hostRead")
    val portRead: Option[Int] = parameters.get("portRead").flatMap(_.toIntOption)
    val userRead: Option[String] = parameters.get("userRead")
    val passwordRead: Option[String] = parameters.get("passwordRead")

    val databaseWrite: Option[String] = parameters.get("databaseWrite")
    val collectionWrite: Option[String] = parameters.get("collectionWrite")
    val hostWrite: Option[String] = parameters.get("hostWrite")
    val portWrite: Option[Int] = parameters.get("portWrite").flatMap(_.toIntOption)
    val userWrite: Option[String] = parameters.get("userWrite")
    val passwordWrite: Option[String] = parameters.get("passwordWrite")

    val params: PPZBMedXml_Parameters = PPZBMedXml_Parameters(xmlOut, databaseRead, collectionRead, hostRead, portRead,
      userRead, passwordRead, databaseWrite, collectionWrite, hostWrite, portWrite, userWrite, passwordWrite)
    val startDate: Date = new Date()

    (new Main).exportXml(params) match {
      case Success(_) =>
        logger.info(timeAtProcessing(startDate))
        System.exit(0)
      case Failure(exception) =>
        logger.error(if (exception.getMessage == "") "Interrupted Processing!" else s"Error: ${exception.getMessage}", exception.toString)
        System.exit(1)
    }
  }

  def timeAtProcessing(startDate: Date): String = {
    val endDate: Date = new Date()
    val elapsedTime: Long = (endDate.getTime - startDate.getTime) / 1000
    val minutes: Long = elapsedTime / 60
    val seconds: Long = elapsedTime % 60
    s"Processing time: ${minutes}min e ${seconds}s\n"
  }
}