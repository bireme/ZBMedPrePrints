package ppzbmedxml

import org.mongodb.scala.bson.{BsonArray, BsonValue}
import org.mongodb.scala.Document

import java.io.BufferedWriter
import java.nio.file.{Files, Paths}
import scala.util.{Failure, Success, Try}
import scala.xml.{Elem, PrettyPrinter, XML}
import scala.collection.JavaConverters._


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


class ZBMedPP{

  def toXml(docsMongo: Seq[Document], pathOut: String): Try[Unit] = {
    Try{
      generateXml(docsMongo.map(f => mapElements(f)), pathOut) match {
        case Success(_) => println(s"\nLoading elements in the file generated in: $pathOut")
        case Failure(_) => println(s"\nXml generation Failed!")
      }
    }
  }

  private def mapElements(doc: Document): ZBMedpp_doc ={

    val id: String = s"ppzbmed-${doc.getString("docLink").split("[/.]").reverse.head}"
    val bd: String = "PREPRINT-ZBMED"
    val instance: String = "regional"
    val collection: String = "09-preprints"
    val typeTmp: String = "preprint"
    val pu: String = doc.getString("source")
    val ti: String = doc.getString("title").replace("<", "&lt;").replace(">","&gt;")
    val doi: String = doc.getString("id")
    val link: Seq[String] = fieldToSeq(doc, "link")
    val linkPdf: Seq[String] = fieldToSeq(doc, "pdfLink")
    val fullText: String = if (link.nonEmpty | linkPdf.nonEmpty) "1" else ""
    val ab: String = doc.getString("abstract").replace("<", "&lt;").replace(">","&gt;")
    val au: Seq[String] = fieldToSeq(doc, "authors")
    val entryDate: String = doc.getString("date").split("T").head.replace("-", "")
    val da: String = entryDate.substring(0,6)

    ZBMedpp_doc(id, bd, instance, collection, typeTmp, pu, ti, doi, link, linkPdf, fullText, ab, au, entryDate, da)
  }

  private def fieldToSeq(doc: Document, nameField: String): Seq[String]={

    doc.get[BsonValue](nameField).get match {
      case field if field.isArray =>
        doc.get[BsonArray](nameField).get.getValues.asScala.map(tag => tag.asString().getValue).toSeq
      case field if field.isString => Seq(doc.getString(nameField))
    }
  }


  private def generateXml(elements: Seq[ZBMedpp_doc], pathOut: String): Try[Unit] = {
    Try{
      val xmlPath: BufferedWriter = Files.newBufferedWriter(Paths.get(pathOut))
      val printer = new PrettyPrinter(80, 2)
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

  private def setElement(name: String, author: String): Elem = {
    <field name={name}>{author}</field>
  }
}