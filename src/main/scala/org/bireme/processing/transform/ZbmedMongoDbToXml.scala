package org.bireme.processing.transform

import ch.qos.logback.classic.ClassicConstants
import org.bireme.processing.tools.models.ZbmedXmlParameters
import org.slf4j.{Logger, LoggerFactory}

import java.util.Date
import scala.util.{Failure, Success}

object ZbmedMongoDbToXml {

  System.setProperty(ClassicConstants.CONFIG_FILE_PROPERTY, "./src/main/scala/resources/logback.xml")
  val logger: Logger = LoggerFactory.getLogger(classOf[ZbmedExportXml])


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
    System.err.println("[--append]                - If present, will compose the collection without clearing it first")
    System.err.println("[-indexName=<name>]     - parameter to determine the name of the field that will take on the role of collection index")
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
    val append: Boolean = parameters.contains("append")
    val indexName: Option[String] = parameters.get("indexName")

    val params: ZbmedXmlParameters = ZbmedXmlParameters(xmlOut, databaseRead, collectionRead, hostRead, portRead,
      userRead, passwordRead, databaseWrite, collectionWrite, hostWrite, portWrite, userWrite, passwordWrite, append, indexName)
    val startDate: Date = new Date()

    (new ZbmedExportXml).exportXml(params) match {
      case Success(_) =>
        logger.info(timeAtProcessing(startDate))
        System.exit(0)
      case Failure(exception) =>
        logger.error(if (exception.getMessage == "()") "Interrupted Processing!" else s"Error: ${exception.toString}")
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