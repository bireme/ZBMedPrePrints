import ch.qos.logback.classic.ClassicConstants
import Main.logger
import org.slf4j.{Logger, LoggerFactory}
import ppzbmedxml.ZBMedPP
import mongodb.MongoExport
import org.mongodb.scala.Document

import java.util.Date
import scala.util.{Failure, Success, Try}


case class PPZBMedXml_Parameters(xmlOut: String,
                                 database: String,
                                 collection: String,
                                 host: Option[String],
                                 port: Option[Int],
                                 user: Option[String],
                                 password: Option[String])


class Main {

  private def exportXml(parameters: PPZBMedXml_Parameters): Try[Unit] = {
    Try {
      System.out.println("\n")
      logger.info(s"Migration started - ZBMed preprints ${new Date()}")

      val zbmedpp = new ZBMedPP
      val mExport: MongoExport = new MongoExport(parameters.database, parameters.collection, parameters.host, parameters.port)
      val docsMongo: Seq[Document] = mExport.findAll

      docsMongo.length match {
        case docs if docs == 0 => throw new Exception(s"${logger.warn("No documents found check collection and parameters")}")
        case docs if docs > 0 => logger.info(s"Connected to mongodb - database: ${parameters.database}, collection: ${parameters.collection}," +
          s" host: ${parameters.host.getOrElse("localhost")}, port: ${parameters.port.get}, user: ${parameters.user.getOrElse("None")}")
          logger.info(s"Total documents: ${docsMongo.length}")
      }
      zbmedpp.toXml(docsMongo, parameters.xmlOut) match {
        case Success(value) =>
          value.zipWithIndex.foreach{
            case (f, index) =>
              mExport.insertDocumentNormalized(f)
              zbmedpp.amountProcessed(value.length, index + 1, if (value.length >= 500) 500 else value.length)
          }
          s"\n${logger.info(s"FILE GENERATED SUCCESSFULLY IN: ${parameters.xmlOut}")}"
        case Failure(_) => logger.warn("FAILURE TO GENERATE FILE")
      }
    }
  }
}

object Main {

  System.setProperty(ClassicConstants.CONFIG_FILE_PROPERTY, "./src/main/scala/resources/logback.xml")
  val logger: Logger = LoggerFactory.getLogger(classOf[Main])


  private def usage(): Unit = {
    System.err.println("-xmlout=<path>     - XML file output directory")
    System.err.println("-database=<name>   - MongoDB database name")
    System.err.println("-collection=<name> - MongoDB database collection name")
    System.err.println("[-host=<name>]     - MongoDB server name. Default value is 'localhost'")
    System.err.println("[-port=<number>]   - MongoDB server port number. Default value is 27017")
    System.err.println("[-user=<name>])    - MongoDB user name")
    System.err.println("[-password=<pwd>]  - MongoDB user password")
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

    if (!Set("xmlout", "database", "collection").forall(parameters.contains)) usage()

    val xmlOut: String = parameters("xmlout")
    val database: String = parameters("database")
    val collection: String = parameters("collection")
    val host: Option[String] = parameters.get("host")
    val port: Option[Int] = parameters.get("port").flatMap(_.toIntOption)
    val user: Option[String] = parameters.get("user")
    val password: Option[String] = parameters.get("password")

    val params: PPZBMedXml_Parameters = PPZBMedXml_Parameters(xmlOut, database, collection, host, port, user, password)
    val startDate: Date = new Date()

    (new Main).exportXml(params) match {
      case Success(_) =>
        logger.info(timeAtProcessing(startDate))
        System.exit(0)
      case Failure(exception) =>
        logger.error(if (exception.getMessage == "()") "Interrupted Processing!" else "Error: ", exception.toString)
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