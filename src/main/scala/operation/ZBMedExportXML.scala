package operation

import com.google.gson.Gson
import models.{PPZBMedXml_Parameters, ZBMedpp_doc}
import services.MongoDB
import org.mongodb.scala.Document
import org.slf4j.{Logger, LoggerFactory}

import java.util.Date
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

class ZBMedExportXML {

  val logger: Logger = LoggerFactory.getLogger(classOf[ZBMedExportXML])

  def exportXml(parameters: PPZBMedXml_Parameters): Try[Unit] = {
    Try {
      System.out.println("\n")
      logger.info(s"Migration started - ZBMed preprints ${new Date()}")

      val mExportRead: MongoDB = new MongoDB(parameters.databaseRead, parameters.collectionRead, parameters.hostRead,
        parameters.portRead, parameters.userRead, parameters.passwordRead, true)

      val mExportWrite: MongoDB = new MongoDB(parameters.databaseWrite.getOrElse(parameters.databaseRead),
        parameters.collectionWrite.getOrElse(parameters.collectionRead), parameters.hostWrite, parameters.portWrite,
        parameters.userWrite, parameters.passwordWrite, parameters.append)

      val docsMongo: Seq[Document] = mExportRead.findAll

      if (existDocumentsOrStop(docsMongo, parameters)) {
        processData(docsMongo, parameters, mExportRead, mExportWrite)
      }
    }
  }

  private def processData(docsMongo: Seq[Document], parameters: PPZBMedXml_Parameters, mExportRead: MongoDB, mExportWrite: MongoDB): Try[Unit] = {
    Try {
      val buffer: ListBuffer[ZBMedpp_doc] = new ListBuffer[ZBMedpp_doc]
      val zbmedpp: ZBMedppToXml = new ZBMedppToXml

      zbmedpp.toXml(docsMongo, parameters.xmlOut) match {
        case Success(value) =>
          logger.info(s"Writing normalized documents in: database: ${parameters.databaseWrite.get}," +
            s" collection: ${parameters.collectionWrite.get}," +
            s" host: ${parameters.hostWrite.getOrElse("localhost")}," +
            s" port: ${parameters.portWrite.getOrElse(27017)}," +
            s" user: ${parameters.userWrite.getOrElse("None")}")

          value.zipWithIndex.foreach {
            case (zbmedpp_doc, index) =>
              zbmedpp.amountProcessed(value.length, index + 1, if (value.length >= 10000) 10000 else value.length)
              buffer.append(zbmedpp_doc)
              if (buffer.length % 1000 == 0 || index + 1 == value.length) {
                insertDocumentNormalized(buffer, mExportWrite, parameters.collectionWrite.get)
                buffer.clear()
              }
          }
          if (buffer.nonEmpty) {
            insertDocumentNormalized(buffer, mExportWrite, parameters.collectionWrite.get)
            buffer.clear()
          }
          mExportRead.close()
          mExportWrite.close()
          logger.info(s"FILE GENERATED SUCCESSFULLY IN: ${parameters.xmlOut}")
        case Failure(_) => logger.warn("FAILURE TO GENERATE FILE")
      }
    }
  }

  def insertDocumentNormalized(docs: ListBuffer[ZBMedpp_doc], mExport: MongoDB, nameCollectionNormalized: String): Unit = {

    val docJson: ListBuffer[String] = docs.map(new Gson().toJson(_))

    if (!mExport.existCollection(nameCollectionNormalized)) {
      mExport.createCollection(nameCollectionNormalized)
    }
    mExport.insertDocumentNormalized(docJson.toList)
  }

  def existDocumentsOrStop(docsMongo: Seq[Document], parameters: PPZBMedXml_Parameters): Boolean = {
    docsMongo.length match {
      case docs if docs == 0 =>
        throw new Exception(s"${logger.warn("No documents found check collection and parameters")}")
        false
      case docs if docs > 0 =>
        logger.info(s"Connected to mongodb - database: ${parameters.databaseRead}, collection: ${parameters.collectionRead}," +
          s" host: ${parameters.hostRead.getOrElse("localhost")}, port: ${parameters.portRead.getOrElse(27017)}, user: ${parameters.userRead.getOrElse("None")}")
        logger.info(s"Total documents: ${docsMongo.length}")
        true
    }
  }
}