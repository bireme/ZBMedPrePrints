import mongodb.MongoExport
import org.mongodb.scala.Document
import ppzbmedxml.ZBMedPP

import java.util.Date
import scala.util.{Failure, Success, Try}


case class PPZBMedXml_Parameters(xmlOut: String,
                                 database: String,
                                 collection: String,
                                 host: Option[String],
                                 port: Option[Int],
                                 user: Option[String],
                                 password: Option[String],
                                )
class Main {

  private def exportXml(parameters: PPZBMedXml_Parameters): Try[Unit] = {
    Try {
      val mExport: MongoExport = new MongoExport(parameters.database, parameters.collection, parameters.host, parameters.port)
      val docsMongo: Seq[Document] = mExport.findAll

      new ZBMedPP().toXml(docsMongo, parameters.xmlOut) match {
        case Success(_) => System.out.println(s"|FILE GENERATED SUCCESSFULLY IN: ${parameters.xmlOut}")
        case Failure(exception) => System.out.println(s"|FAILURE TO GENERATE FILE: $exception")
      }
    }
  }
}

object Main {
  private def usage(): Unit = {
    System.err.println("-database=<name>   - MongoDB database name")
    System.err.println("-collection=<name> - MongoDB database collection name")
    System.err.println("-xmlout=<path>     - XML file output directory")
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
        val endDate: Date = new Date()
        val elapsedTime: Long = endDate.getTime - startDate.getTime
        val seconds0: Long = elapsedTime / 1000
        val minutes: Long = seconds0 / 60
        val seconds: Long = seconds0 % 60
        println(s"|Processing time: ${minutes}min e ${seconds}s\n")
        System.exit(0)
      case Failure(exception) =>
        println(s"|Error: ${exception.toString}\n")
        System.exit(1)
    }
  }
}