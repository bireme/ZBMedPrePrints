package org.bireme.processing.`export`

import org.bireme.processing.tools.models.ZBMedpp_doc
import org.bson.Document
import org.slf4j.{Logger, LoggerFactory}

import java.io.{BufferedWriter, File}
import java.nio.charset.Charset
import java.nio.file.Files
import scala.reflect.runtime.currentMirror
import scala.reflect.runtime.universe.TermName
import scala.util.{Failure, Success, Try}
import scala.xml.{Elem, PrettyPrinter, XML}

class ZBMedppToXml {

  val logger: Logger = LoggerFactory.getLogger(classOf[ZBMedppToXml])

  def normalizeData(docsMongo: Seq[Document], pathOut: String): Try[Iterator[ZBMedpp_doc]] = {

    logger.info("+++Normalization process started")
    var index = 0
    val zbmedppIteratorList: Try[Iterator[ZBMedpp_doc]] = generateXml(docsMongo.map {
     f => mapElements(f) match {
          case Success(value) =>
            index += 1
            amountProcessed(docsMongo.length, index, if (docsMongo.length >= 10000) 10000 else docsMongo.length)
            value
          case Failure(exception) => throw new Exception(logger.error(s"_id Document in Mongodb: ${f.get("_id")} Exception: ", exception).toString)
        }
    }, pathOut)
    logger.info("---Completed normalization process")
    zbmedppIteratorList
  }

  private def mapElements(doc: Document): Try[ZBMedpp_doc] = {
    Try {
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
      val au: Array[String] = if (doc.containsKey("authors")) fieldToSeq(doc, "authors") else fieldToSeq(doc, "rel_authors")
      val mj: Array[String] = if (doc.containsKey("all_annotations")) fieldToSeq(doc, "all_annotations") else Array("")

      ZBMedpp_doc(id, alternateId, db, instance, collection, typeTmp, la, fo, dp, pu, ti, aid, link, linkPdf, fullText, ab, au, entryDate, da, mj)
    }
  }

  def getId(doc: Document): String = {
    val id = if (doc.getString("id") == null) {
      logger.warn(s"ID Null")
      ""
    } else doc.getString("id")
    id match {
      case id if id.nonEmpty => id
      case id if id.isEmpty => logger.warn(s"Not Found id: _id mongodb ${doc.get("_id")}")
        s"${logger.info("Processing documents...")}"
    }
  }

  private def getIdAlternate(source: String, id: String): String = {

    /** sources: medrxiv, biorxiv, arxiv, researchsquare, ssrn, chemrxiv, preprints.org, psyarxiv, biohackrxiv, beilstein archives, authorea preprints */
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

  private def fieldToSeq(doc: Document, nameField: String): Array[String] = {

    doc.get(nameField) match {
      case value if value.isInstanceOf[String] => Array(doc.getString(nameField))
      case _ =>
        val underlyingOption = Try {
          val instanceMirror = currentMirror.reflect(doc.get(nameField))
          val underlyingField = instanceMirror.symbol.typeSignature.member(TermName("underlying")).asTerm
          instanceMirror.reflectField(underlyingField).get
        }.toOption
        underlyingOption match {
          case Some(underlyingList) => underlyingList match {
            case any: List[Any] => any.toArray.map(f => getArrayValues(f, nameField)).filterNot(_.isEmpty)
          }
          case None => Array("")
        }
    }
  }

  def getArrayValues(any: Any, name: String): String = {

    val value: String = any match {
      case any: String => any
      case anyD: Document =>
        if (name == "all_annotations")
          anyD.getString("mfn")
        else if (name == "rel_authors")
          anyD.getString("author_name")
        else anyD.getString(name)
    }
    if (value == null) ""
    else if (name == "all_annotations") "^d".concat(value)
    else value
  }

  private def generateXml(elements: Seq[ZBMedpp_doc], pathOut: String): Try[Iterator[ZBMedpp_doc]] = {
    Try {
      val xmlPath: BufferedWriter = Files.newBufferedWriter(new File(pathOut).toPath, Charset.forName("utf-8"))
      val printer: PrettyPrinter = new PrettyPrinter(50000, 0)

      val xmlFormat: Elem =
        <add>
          {elements.map(f => docToElem(f))}
        </add>

      XML.write(xmlPath, XML.loadString(printer.format(xmlFormat)), "utf-8", xmlDecl = true, null)
      xmlPath.flush()
      xmlPath.close()

      elements.iterator
    }
  }

  private def docToElem(fields: ZBMedpp_doc): Elem = {

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