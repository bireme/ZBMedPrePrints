package operation

import models.ZBMedpp_doc
import org.mongodb.scala.Document
import org.mongodb.scala.bson.{BsonArray, BsonString, BsonValue}
import org.slf4j.{Logger, LoggerFactory}

import java.io.{BufferedWriter, File}
import java.nio.charset.Charset
import java.nio.file.Files
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}
import scala.xml.{Elem, PrettyPrinter, XML}

class ZBMedppToXml {

  val logger: Logger = LoggerFactory.getLogger(classOf[ZBMedppToXml])

  def toXml(docsMongo: Seq[Document] , pathOut: String): Try[Seq[ZBMedpp_doc]] = {
    Try{
      logger.info("+++Normalization process started")
      val elem: Try[Seq[ZBMedpp_doc]] = generateXml(docsMongo.zipWithIndex.map{
        case (f, index) =>
          mapElements(f) match {
          case Success(value) => amountProcessed(docsMongo.length, index + 1, if (docsMongo.length >= 10000) 10000 else docsMongo.length)
            value
          case Failure(exception) => throw new Exception(logger.error(s"_id Document in Mongodb: ${f.get("_id").get.asObjectId().getValue} Exception: ", exception).toString)
        }
      }, pathOut)
      logger.info("---Completed normalization process")
      elem.get
    }
  }

  private def mapElements(doc: Document): Try[ZBMedpp_doc] ={
    Try{
      val idValidated: String = getId(doc)
      val id: String = s"ppzbmed-$idValidated".replace("/", ".")
      val alternateId: String = getIdAlternate(doc.getString("source"), id)
      val db: String = s"PREPRINT-${doc.getString("source").toUpperCase}"
      val instance: String = "regional"
      val collection: String = "09-preprints"
      val typeTmp: String = "preprint"
      val la: String = "en"
      val pu: String = doc.getString("source")
      val entryDate: String = doc.getString("date").split("T").head.replace("-", "")
      val da: String = entryDate.substring(0, 6)
      val dp: String = entryDate.substring(0, 4)
      val fo: String = s"$pu; $dp."
      val ti: String = doc.getString("title").concat(" (preprint)").replace("<", "&lt;").replace(">", "&gt;")
      val aid: String = idValidated
      val link: Array[String] = fieldToSeq(doc, "link").filter(_ != doc.getString("pdfLink"))
      val linkPdf: Array[String] = fieldToSeq(doc, "pdfLink")
      val fullText: String = if (link.nonEmpty | linkPdf.nonEmpty) "1" else ""
      val ab: String = doc.getString("abstract").replace("<", "&lt;").replace(">", "&gt;")
      val au: Array[String] = if (doc.get[BsonValue]("authors").isDefined) fieldToSeq(doc, "authors") else fieldToSeq(doc, "rel_authors")
      val mj: Array[String] = if (doc.get[BsonValue]("all_annotations").isDefined) fieldToSeq(doc, "all_annotations") else Array("")

      ZBMedpp_doc(id, alternateId, db, instance, collection, typeTmp, la, fo, dp, pu, ti, aid, link, linkPdf, fullText, ab, au, entryDate, da, mj)
    }
  }

  def getId(doc: Document): String ={
    val id = if (doc.getString("id") == null) {
      logger.warn(s"ID Null")
      ""
    } else doc.getString("id")
    id match {
      case id if id.nonEmpty => id
      case id if id.isEmpty => logger.warn(s"Not Found id: _id mongodb ${doc.get("_id").get.asObjectId().getValue}")
        s"${logger.info("Processing documents...")}"
    }
  }

  private def getIdAlternate(source: String, id: String): String = {

    /**sources: medrxiv, biorxiv, arxiv, researchsquare, ssrn, chemrxiv, preprints.org, psyarxiv, biohackrxiv, beilstein archives, authorea preprints*/
    source match {
      case "medrxiv" => if (id.nonEmpty) {
        s"ppmedrxiv-${id.split("[/.]").reverse.head}"
      } else ""
      case "biorxiv" => if (id.nonEmpty) {
        s"ppbiorxiv-${id.split("[/.]").reverse.head}"
      } else ""
      case _ => ""
    }
  }

  private def fieldToSeq(doc: Document, nameField: String): Array[String]={

    doc.get[BsonValue](nameField).get match {
      case field if field.isArray =>
        nameField match {
          case "rel_authors" =>
            val authorsRel: Iterable[Iterable[String]] = doc.get[BsonValue](nameField).get.asArray().asScala.map(f =>
              f.asDocument().entrySet().asScala.map(f => f.getValue.asString().getValue))
            val authors: Iterable[String] = for {ad <- authorsRel
                                                 a <- ad}
                                            yield a
            authors.toArray.filter(f => f.nonEmpty)
          case "all_annotations" => getMfn(doc, nameField)
          case _ => doc.get[BsonArray](nameField).get.getValues.asScala.map(tag => tag.asString().getValue).toArray
        }
      case _ => Array(doc.getString(nameField))
    }
  }

  def getMfn(doc: Document, nameField: String): Array[String] = {

    val resultDocsAnnotations: mutable.Seq[BsonValue] = doc.get[BsonArray](nameField).get.asArray().asScala
    val resultAnnotationsMfn: Array[Any] = resultDocsAnnotations.map(f => if (f.isDocument) f.asDocument().getOrDefault("mfn", BsonString("()")).asString().getValue).toArray

    resultAnnotationsMfn.map(f => if (f.toString != "()") "^d".concat(f.toString) else f.toString.replace("()", ""))
  }

  private def generateXml(elements: Seq[ZBMedpp_doc], pathOut: String): Try[Seq[ZBMedpp_doc]] = {
    Try{
      val xmlPath: BufferedWriter = Files.newBufferedWriter(new File(pathOut).toPath, Charset.forName("utf-8"))
      val printer: PrettyPrinter = new PrettyPrinter(50000, 0)

      val xmlFormat: Elem =
        <add>
          {elements.map(f => docToElem(f))}
        </add>

      XML.write(xmlPath, XML.loadString(printer.format(xmlFormat)), "utf-8", xmlDecl = true, null)
      xmlPath.flush()
      xmlPath.close()

      elements
    }
  }

  private def docToElem(fields: ZBMedpp_doc): Elem ={

//    {if (fields.urPdf.nonEmpty) {fields.urPdf.map(f => setElement("ur", f))} else xml.NodeSeq.Empty}

  <doc>
    {if (fields.id.nonEmpty) setElement("id", fields.id) else xml.NodeSeq.Empty}
    {if (fields.alternateId.nonEmpty) setElement("alternate_id", fields.alternateId) else xml.NodeSeq.Empty}
    {if (fields.dbSource.nonEmpty) setElement("db", fields.dbSource) else xml.NodeSeq.Empty}
    {if (fields.instance.nonEmpty) setElement("instance", fields.instance) else xml.NodeSeq.Empty}
    {if (fields.collection.nonEmpty) setElement("collection", fields.collection) else xml.NodeSeq.Empty}
    {if (fields.pType.nonEmpty) setElement("type", fields.pType) else xml.NodeSeq.Empty}
    {if (fields.la.nonEmpty) setElement("la", fields.la) else xml.NodeSeq.Empty}
    {if (fields.fo.nonEmpty) setElement("fo", fields.fo) else xml.NodeSeq.Empty}
    {if (fields.dp.nonEmpty) setElement("dp", fields.dp) else xml.NodeSeq.Empty}
    {if (fields.pu.nonEmpty) setElement("pu", fields.pu) else xml.NodeSeq.Empty}
    {if (fields.ti.nonEmpty) setElement("ti", fields.ti) else xml.NodeSeq.Empty}
    {if (fields.aid.nonEmpty) setElement("aid", fields.aid) else xml.NodeSeq.Empty}
    {if (fields.ur.nonEmpty) {fields.ur.map(f => if (f.nonEmpty) setElement("ur", f))} else xml.NodeSeq.Empty}
    {if (fields.fulltext.nonEmpty) setElement("fulltext", fields.fulltext) else xml.NodeSeq.Empty}
    {if (fields.ab.nonEmpty) setElement("ab", fields.ab) else xml.NodeSeq.Empty}
    {if (fields.au.nonEmpty) {fields.au.map(f => if (f.nonEmpty) setElement("au", f))} else xml.NodeSeq.Empty}
    {if (fields.entryDate.nonEmpty) setElement("entry_date", fields.entryDate) else xml.NodeSeq.Empty}
    {if (fields.da.nonEmpty) setElement("da", fields.da) else xml.NodeSeq.Empty}
    {if (fields.mj.nonEmpty) {fields.mj.map(f => if (f.nonEmpty) setElement("mj", f))} else xml.NodeSeq.Empty}
  </doc>
  }

  private def setElement(name: String, field: String): Elem = <field name={name}>{field}</field>

  def amountProcessed(total: Int, qtdProccess: Int, setAmount: Int): Unit = {
    val valuePrints: Double = total / setAmount
    if (total / qtdProccess <= valuePrints) {
      if (qtdProccess % setAmount == 0 || qtdProccess == total)
      logger.info(s"+++$qtdProccess")
    }
  }

//  def percentageProcessed(total: Int, qtdProccess: Int, perCent: Int): Unit = {
//    val valuePerCent = (perCent * total) / 100
//    if (qtdProccess % valuePerCent == 0) {
//      val value = (qtdProccess.toDouble / total) * 100
//      logger.info(s"+++${Math.round(value)}%")
//    }
//  }
}