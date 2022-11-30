package ppzbmedxml

import org.mongodb.scala.bson.BsonArray
import org.mongodb.scala.Document

import java.io.BufferedWriter
import java.nio.file.{Files, Paths}
import scala.util.{Failure, Success, Try}
import scala.xml.{Elem, PrettyPrinter, XML}
import scala.collection.JavaConverters._


case class ZBMedPp_doc(id: String,
                       db: String,
                       instance: String,
                       collection: String,
                       pType: String,
                       //la: String,                             ***Dado indisponível***
                       pu: String,
                       ti: String,
                       doi: String,
                       ur: String,
                       urPdf: String,
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
        case Success(_) => println(s"\nLoading elements in the file generated in:: $pathOut")
        case Failure(_) => println(s"\nXml generation Failed!")
      }
    }
  }

  def mapElements(doc: Document): ZBMedPp_doc ={

    val id: String = s"ppzbmed-${doc.getString("docLink").split("[/.]").reverse.head}"
    val bd: String = "PREPRINT-ZBMED"
    val instance: String = "regional"
    val collection: String = "09-preprints"
    val typeTmp: String = "preprint"
    val pu: String = doc.getString("source")
    val ti: String = doc.getString("title").replace("<", "&lt;").replace(">","&gt;")
    val doi: String = doc.getString("id")
    val link: String = doc.getString("link")
    val linkPdf: String = doc.getString("pdfLink")
    val fullText: String = if (link.nonEmpty | linkPdf.nonEmpty) "1" else ""
    val ab: String = doc.getString("abstract").replace("<", "&lt;").replace(">","&gt;")
    val au: Seq[String] = doc.get[BsonArray]("authors").get.getValues.asScala.map(tag => tag.asString().getValue).toSeq
    val entryDate: String = doc.getString("date").split("T").head.replace("-", "")
    val da: String = entryDate.substring(0,6)

    ZBMedPp_doc(id, bd, instance, collection, typeTmp, pu, ti, doi, link, linkPdf, fullText, ab, au, entryDate, da)
  }

  def generateXml(elements: Seq[ZBMedPp_doc], pathOut: String): Try[Unit] = {
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

  def docToElem(fields: ZBMedPp_doc): Elem ={

  <doc>
    <field name={"id"}>{fields.id}</field>
    <field name={"db"}>{fields.db}</field>
    <field name={"instance"}>{fields.instance}</field>
    <field name={"collection"}>{fields.collection}</field>
    <field name={"type"}>{fields.pType}</field>
    <field name={"pu"}>{fields.pu}</field>
    <field name={"ti"}>{fields.ti}</field>
    <field name={"doi"}>{fields.doi}</field>
    <field name={"ur"}>{fields.ur}</field>
    <field name={"ur"}>{fields.urPdf}</field>
    <field name={"fulltext"}>{fields.fulltext}</field>
    <field name={"ab"}>{fields.ab}</field>
    {fields.au.map(f => setAuthorsElem(f))}
    <field name={"entry_date"}>{fields.entryDate}</field>
    <field name={"da"}>{fields.da}</field>
  </doc>
  }

  def setAuthorsElem(author: String): Elem = {
    <field name={"au"}>{author}</field>
  }
}