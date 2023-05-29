package operation

import com.google.gson.Gson
import models.{PPZBMedXml_Parameters, ZBMedpp_doc}
import services.MongoDB
import org.mongodb.scala.Document
import org.slf4j.{Logger, LoggerFactory}

import java.util.Date
import scala.util.{Failure, Success, Try}

class ZBMedExportXML {

  val logger: Logger = LoggerFactory.getLogger(classOf[ZBMedExportXML])

  def exportXml(parameters: PPZBMedXml_Parameters): Try[Unit] = {
    Try {
      System.out.println("\n")
      logger.info(s"Migration started - ZBMed preprints ${new Date()}")

      val zbmedpp = new ZBMedppToXml
      val mExportRead: MongoDB = new MongoDB(parameters.databaseRead, parameters.collectionRead, parameters.hostRead, parameters.portRead)
      val mExportWrite: MongoDB = new MongoDB(parameters.databaseWrite.getOrElse(parameters.databaseRead),
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

  def insertDocumentNormalized(doc: ZBMedpp_doc, mExport: MongoDB, nameCollectionNormalized: String): Unit = {

    val docJson: String = new Gson().toJson(doc)

    if (!mExport.existCollection(nameCollectionNormalized)) {
      mExport.createCollection(nameCollectionNormalized)
    }
    val isFieldRepeted: Boolean = mExport.isIdRepetedNormalized("id", doc.id)
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