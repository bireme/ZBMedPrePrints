package ppzbmedxml

import org.mongodb.scala.Document
import org.mongodb.scala.bson.{BsonArray, BsonValue}
import org.slf4j.{Logger, LoggerFactory}

import java.io.{BufferedWriter, File}
import java.nio.charset.Charset
import java.nio.file.Files
import java.util.Date
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}
import scala.xml.{Elem, PrettyPrinter, XML}

private case class ZBMedpp_doc(id: String,
                               alternateId: String,
                               db: String,
                               dbSource: String,
                               instance: String,
                               collection: String,
                               pType: String,
                               //la: String,                             ***Dado indisponível***
                               pu: String,
                               ti: String,
                               aid: String,
                               ur: Seq[String],
                               urPdf: Seq[String],
                               fulltext: String,
                               ab: String,
                               au: Seq[String],
                               //afiliacaoAutor: String,                 ***Dado indisponível***
                               entryDate: String,
                               da: String)
                               //versionMedrxivBiorxiv: String,          ***Dado indisponível***
                               // license: String,                       ***Dado indisponível***
                               //typeDocumentMedrxivBiorxiv: String,     ***Dado indisponível***
                               //categoryMedrxivBiorxiv: String)         ***Dado indisponível***

class ZBMedPP {

  val logger: Logger = LoggerFactory.getLogger(classOf[ZBMedPP])

  def toXml(docsMongo: Seq[Document] , pathOut: String): Try[Unit] = {
    Try{
      System.out.println("\n")
      logger.info(s"Migration started - ZBMed preprints ${new Date()}")
      logger.info(s"Total documents: ${docsMongo.length}")

      docsMongo.length match {
        case docs if docs == 0 => throw new Exception(s"${logger.warn("No documents found check collection and parameters")}")
        case docs if docs > 0 => logger.info("Connected to mongodb - Processing documents...")
          generateXml(docsMongo.map(f => mapElements(f) match {
          case Success(value) => logger.debug(s"Exported document to XML file ${f.get("_id").get.toString}")
            value
          case Failure(exception) =>
            throw new Exception(logger.error(s"_id Document in Mongodb: ${f.get("_id").get.toString} Exception: ", exception).toString)
        }), pathOut)
      }
    }
  }

  private def mapElements(doc: Document): Try[ZBMedpp_doc] ={
    Try{

      val id: String = s"ppzbmed-${doc.get("_id").get.asObjectId().getValue}"
      val alternateId: String = getIdAlternate(doc)
      val bd: String = "PREPRINT-ZBMED"
      val bdSource: String = s"PREPRINT-${doc.getString("source").toUpperCase}"
      val instance: String = "regional"
      val collection: String = "09-preprints"
      val typeTmp: String = "preprint"
      val pu: String = doc.getString("source")
      val ti: String = doc.getString("title").replace("<", "&lt;").replace(">", "&gt;")
      val aid: String = doc.getString("id")
      val link: Seq[String] = fieldToSeq(doc, "link").filter(_ != doc.getString("pdfLink"))
      val linkPdf: Seq[String] = fieldToSeq(doc, "pdfLink")
      val fullText: String = if (link.nonEmpty | linkPdf.nonEmpty) "1" else ""
      val ab: String = doc.getString("abstract").replace("<", "&lt;").replace(">", "&gt;")
      val au: Seq[String] = if (doc.get[BsonValue]("authors").isDefined) fieldToSeq(doc, "authors") else fieldToSeq(doc, "rel_authors")
      val entryDate: String = doc.getString("date").split("T").head.replace("-", "")
      val da: String = entryDate.substring(0, 6)

      ZBMedpp_doc(id, alternateId, bd, bdSource, instance, collection, typeTmp, pu, ti, aid, link, linkPdf, fullText, ab, au, entryDate, da)
    }
  }

  private def getIdAlternate(doc: Document): String = {
    val source: String = doc.getString("source")
    /**sources: medrxiv, biorxiv, arxiv, researchsquare, ssrn, chemrxiv, preprints.org, psyarxiv, biohackrxiv, beilstein archives, authorea preprints*/
    source match {
      case "medrxiv" => s"ppmedrxiv-${doc.getString("id").split("[/.]").reverse.head}" //"Ex.'id' = 10.1101/2020.05.08.20092080"
      case "biorxiv" => s"ppbiorxiv-${doc.getString("id").split("[/.]").reverse.head}" //"Ex.'id' = 10.1101/2020.04.05.026146"
      case _ => s"ppzbmed-${doc.get("_id")}"
    }
  }

  private def fieldToSeq(doc: Document, nameField: String): Seq[String]={

    doc.get[BsonValue](nameField).get match {
      case field if field.isString => Seq(doc.getString(nameField))
      case field if field.isArray =>
        nameField match {
          case "rel_authors" =>
            val authorsRel: Iterable[Iterable[String]] = doc.get[BsonValue](nameField).get.asArray().asScala.map(f =>
              f.asDocument().entrySet().asScala.map(f => f.getValue.asString().getValue))
            val authors: Iterable[String] = for {ad <- authorsRel
                                                 a <- ad}
                                            yield a
            authors.toSeq.filter(f => f.nonEmpty)
          case _ => doc.get[BsonArray](nameField).get.getValues.asScala.map(tag => tag.asString().getValue).toSeq
        }
    }
  }

  private def generateXml(elements: Seq[ZBMedpp_doc], pathOut: String): Try[Unit] = {
    Try{
      val xmlPath: BufferedWriter = Files.newBufferedWriter(new File(pathOut).toPath, Charset.forName("utf-8"))
      val printer: PrettyPrinter = new PrettyPrinter(50000, 0)

      val xmlFormat: Elem =
        <add>
          {elements.map(f => docToElem(f))}
        </add>

//      xmlPath.write(s"<?xml version=\"${1.0}\" encoding=\"${"utf-8"}\"?>\n")
//      xmlPath.write(XML.loadString(printer.format(xmlFormat)).toString())

      XML.write(xmlPath, XML.loadString(printer.format(xmlFormat)), "utf-8", xmlDecl = true, null)
      xmlPath.flush()
      xmlPath.close()
    }
  }

  private def docToElem(fields: ZBMedpp_doc): Elem ={

  <doc>
    <field name={"id"}>{fields.id}</field>
    <field name={"alternate_id"}>{fields.alternateId}</field>
    <field name={"db"}>{fields.db}</field>
    <field name={"db"}>{fields.dbSource}</field>
    <field name={"instance"}>{fields.instance}</field>
    <field name={"collection"}>{fields.collection}</field>
    <field name={"type"}>{fields.pType}</field>
    <field name={"pu"}>{fields.pu}</field>
    <field name={"ti"}>{fields.ti}</field>
    <field name={"aid"}>{fields.aid}</field>
    {fields.ur.map(f => setElement("ur", f))}
    {fields.urPdf.map(f => setElement("ur", f))}
    <field name={"fulltext"}>{fields.fulltext}</field>
    <field name={"ab"}>{fields.ab}</field>
    {fields.au.map(f => setElement("au", f))}
    <field name={"entry_date"}>{fields.entryDate}</field>
    <field name={"da"}>{fields.da}</field>
  </doc>
  }

  private def setElement(name: String, field: String): Elem = <field name={name}>{field}</field>
}