package ppzbmedxml

import org.mongodb.scala.bson.{BsonArray, BsonValue}
import org.mongodb.scala.Document

import java.io.BufferedWriter
import java.nio.file.{Files, Paths}
import java.util.Date
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}
import scala.xml.{Elem, PrettyPrinter, XML}


private case class ZBMedpp_doc(id: String,
                               db: String,
                               instance: String,
                               collection: String,
                               pType: String,
                               //la: String,                             ***Dado indisponível***
                               pu: String,
                               ti: String,
                               doi: String,
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

class ZBMedPP extends Enumeration {


  def toXml(docsMongo: Seq[Document], pathOut: String): Try[Unit] = {
    Try{

      System.out.println(s"\n|ZBMed preprints - Migration started: ${new Date()}")
      System.out.println(s"|Total documents: ${docsMongo.length}")

      generateXml(docsMongo.map(f => mapElements(f) match {
        case Success(value) => value
        case Failure(exception) => throw new Exception(s"$exception: _id Document in Mongodb: ${f.get("_id").get.asObjectId().getValue}")
      }), pathOut)
    }
  }

  private def mapElements(doc: Document): Try[ZBMedpp_doc] ={
    Try{

      val id: String = getId(doc)
      val bd: String = "PREPRINT-ZBMED"
      val instance: String = "regional"
      val collection: String = "09-preprints"
      val typeTmp: String = "preprint"
      val pu: String = doc.getString("source")
      val ti: String = doc.getString("title").replace("<", "&lt;").replace(">", "&gt;")
      val doi: String = doc.getString("id")
      val link: Seq[String] = fieldToSeq(doc, "link").filter(_ != doc.getString("pdfLink"))
      val linkPdf: Seq[String] = fieldToSeq(doc, "pdfLink")
      val fullText: String = if (link.nonEmpty | linkPdf.nonEmpty) "1" else ""
      val ab: String = doc.getString("abstract").replace("<", "&lt;").replace(">", "&gt;")
      val au: Seq[String] = if (doc.get[BsonValue]("authors").isDefined) fieldToSeq(doc, "authors") else fieldToSeq(doc, "rel_authors")
      val entryDate: String = doc.getString("date").split("T").head.replace("-", "")
      val da: String = entryDate.substring(0, 6)

      ZBMedpp_doc(id, bd, instance, collection, typeTmp, pu, ti, doi, link, linkPdf, fullText, ab, au, entryDate, da)
    }
  }

  private def getId(doc: Document): String = {

    val source: String = doc.getString("source")

    source match {
      case "biorxiv" => s"ppzbmed-${doc.getString("id").split("[/.]").reverse.head}" //"Ex.'id' = 10.1101/2020.04.05.026146"
      case "arxiv" => s"ppzbmed-${doc.getString("id").split("[.]").reverse.head.replace("v", "")}" //"Ex.'id' = 2005.11008v1"
      case "researchsquare" => s"ppzbmed-${doc.getString("id").split("[/.]").reverse(1).split("-").reverse.head}" //"Ex.'id' = 10.21203/rs.3.rs-41695/v1" ///&&&&VALIDAR
      case "ssrn" => s"ppzbmed-${doc.getString("id").split("[/.]").reverse.head}" //"Ex.'id' = 10.2139/ssrn.3549193"
      case "medrxiv" => s"ppzbmed-${doc.getString("id").split("[/.]").reverse.head}" //"Ex.'id' = 10.1101/2020.05.08.20092080"
      case "chemrxiv" => s"ppzbmed-${doc.getString("id").split("[/.]").reverse(1)}" //"Ex.'id' = 10.26434/chemrxiv.11936292.v1"
      case "preprints.org" => s"ppzbmed-${doc.getString("id").split("[/.]").reverse(1)}" //"Ex.'id' = 10.20944/preprints202004.0204.v1"
      case "psyarxiv" => s"ppzbmed-${doc.getString("id").split("[/.]").reverse.head}" //"Ex.'id' = 10.31234/osf.io/52bw4"
      case "biohackrxiv" => s"ppzbmed-${doc.getString("id").split("[/.]").reverse.head}" //"Ex.'id' = 10.37044/osf.io/9d3cz"
      case "beilstein archives" => s"ppzbmed-${doc.getString("id").split("[/.]").reverse(1)}" //"Ex.'id' = 10.3762/bxiv.2020.136.v1"
      case "authorea preprints" => s"ppzbmed-${doc.getString("id").split("[/.]").reverse(1).split("-").reverse.head}" //"Ex.'id' = 10.22541/au.160674522.20049875/v1"
      case _ => throw new Exception(s"ID NOT MAPPED TO $source")
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
      val xmlPath: BufferedWriter = Files.newBufferedWriter(Paths.get(pathOut))
      val printer = new PrettyPrinter(50000, 2)
      val xmlFormat =
        <add>
          {elements.map(f => docToElem(f))}
        </add>

      XML.write(xmlPath, XML.loadString(printer.format(xmlFormat)), "UTF-8", xmlDecl = true, null)
      xmlPath.flush()
      xmlPath.close()
    }
  }

  private def docToElem(fields: ZBMedpp_doc): Elem ={

  <doc>
    <field name={"id"}>{fields.id}</field>
    <field name={"db"}>{fields.db}</field>
    <field name={"instance"}>{fields.instance}</field>
    <field name={"collection"}>{fields.collection}</field>
    <field name={"type"}>{fields.pType}</field>
    <field name={"pu"}>{fields.pu}</field>
    <field name={"ti"}>{fields.ti}</field>
    <field name={"doi"}>{fields.doi}</field>
    {fields.ur.map(f => setElement("ur", f))}
    {fields.urPdf.map(f => setElement("ur", f))}
    <field name={"fulltext"}>{fields.fulltext}</field>
    <field name={"ab"}>{fields.ab}</field>
    {fields.au.map(f => setElement("au", f))}
    <field name={"entry_date"}>{fields.entryDate}</field>
    <field name={"da"}>{fields.da}</field>
  </doc>
  }

  private def setElement(name: String, field: String): Elem = {
    <field name={name}>{field}</field>
  }
}