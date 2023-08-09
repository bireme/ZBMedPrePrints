package operation

import models.ZBMedpp_doc
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

  def toXml(docsMongo: Seq[Document], pathOut: String): Try[Iterator[ZBMedpp_doc]] = {

    var index = 0
    generateXml(docsMongo.map {
     f =>

        mapElements(f) match {
          case Success(value) =>
            index += 1
            amountProcessed(docsMongo.length, index, if (docsMongo.length >= 10000) 10000 else docsMongo.length)
            value
          case Failure(exception) => throw new Exception(logger.error(s"_id Document in Mongodb: ${f.get("_id")} Exception: ", exception).toString)
        }
    }, pathOut)

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
      val ab: String = doc.getString("abstract").toString.replace("<", "&lt;").replace(">", "&gt;")
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
      //case str: String => Array(doc.getString(nameField))
      case _ =>
        val underlyingOption = Try {
          val instanceMirror = currentMirror.reflect(doc.get(nameField))
          val underlyingField = instanceMirror.symbol.typeSignature.member(TermName("underlying")).asTerm
          instanceMirror.reflectField(underlyingField).get
        }.toOption
        underlyingOption match {
          case Some(underlyingList) => underlyingList match {

            case any: List[Any] =>
              val h = any.toArray.map(f => mfnTest(f, nameField))
              h
            //case arrayAny: Array[Any] => arrayAny.map(_.toString)
            //case other => other.toString
          }
          case None => Array("")
        }
    }
  }

  def mfnTest(any: Any, name: String) = {

    val h = any match {
      case any: String => any
      case anyD: Document =>
        if (name == "all_annotations")
          anyD.getString("mfn")
        else if (name == "rel_authors")
          anyD.getString("author_name")
        else anyD.getString(name)
//        name match {
//          case "pdfLink" => anyD.getString(name)
//          case "mfn" => anyD.getString(name)
//          case "link" => anyD.getString(name)
//          case "all_annotations" => anyD.getString("mfn")
//          case "authors" => anyD.getString(name)
//          case "rel_authors" => anyD.getString("author_name")
//        }
//
//        anyD.getString("author_name")
//        anyD.getString("rel_authors")
    }
    if (h == null) ""
    else if (name == "all_annotations")
      "^".concat(h)
    else h
  }

//  private def fieldToSeq(doc: Document, nameField: String): Array[String]={
//
//    doc.get[BsonValue](nameField).get match {
//      case field if field.isArray =>
//        nameField match {
//          case "rel_authors" =>
//            val authorsRel: Iterable[Iterable[String]] = doc.get[BsonValue](nameField).get.asArray().asScala.map(f =>
//              f.asDocument().entrySet().asScala.map(f => f.getValue.asString().getValue))
//            val authors: Iterable[String] = for {ad <- authorsRel
//                                                 a <- ad}
//                                            yield a
//            authors.toArray.filter(f => f.nonEmpty)
//          case "all_annotations" => getMfn(doc, nameField)
//          case _ => doc.get[BsonArray](nameField).get.getValues.asScala.map(tag => tag.asString().getValue).toArray
//        }
//      case _ => Array(doc.getString(nameField))
//    }
//  }

  def getMfn(doc: Document, nameField: String): Array[String] = {

    val resultDocsAnnotations: Array[String] = doc.getString(nameField).toArray.map(_.toString)
    //    val resultAnnotationsMfn = resultDocsAnnotations.map(f => if (f.isDocument) f.asDocument().getOrDefault("mfn", BsonString("()")).asString().getValue).toArray
    //
    //    resultAnnotationsMfn.map(f => if (f.toString != "()") "^d".concat(f.toString) else f.toString.replace("()", ""))
    resultDocsAnnotations
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
    //{if (fields.au.nonEmpty) {fields.au.map(f => if (f.nonEmpty) setElement("au", f))} else xml.NodeSeq.Empty}

    <doc>
      {if (fields.id.nonEmpty || fields.id != null) setElement("id", fields.id) else xml.NodeSeq.Empty}
      {if (fields.alternateId.nonEmpty || fields.alternateId != null) setElement("alternate_id", fields.alternateId) else xml.NodeSeq.Empty}
      {if (fields.dbSource.nonEmpty || fields.dbSource != null) setElement("db", fields.dbSource) else xml.NodeSeq.Empty}
      {if (fields.instance.nonEmpty || fields.instance != null) setElement("instance", fields.instance) else xml.NodeSeq.Empty}
      {if (fields.collection.nonEmpty || fields.collection != null) setElement("collection", fields.collection) else xml.NodeSeq.Empty}
      {if (fields.pType.nonEmpty || fields.pType != null) setElement("type", fields.pType) else xml.NodeSeq.Empty}
      {if (fields.la.nonEmpty || fields.la != null) setElement("la", fields.la) else xml.NodeSeq.Empty}
      {if (fields.fo.nonEmpty || fields.fo != null) setElement("fo", fields.fo) else xml.NodeSeq.Empty}
      {if (fields.dp.nonEmpty || fields.dp != null) setElement("dp", fields.dp) else xml.NodeSeq.Empty}
      {if (fields.pu.nonEmpty || fields.pu != null) setElement("pu", fields.pu) else xml.NodeSeq.Empty}
      {if (fields.ti.nonEmpty || fields.ti != null) setElement("ti", fields.ti) else xml.NodeSeq.Empty}
      {if (fields.aid.nonEmpty || fields.aid != null) setElement("aid", fields.aid) else xml.NodeSeq.Empty}
      {if (fields.ur.nonEmpty || fields.ur != null) fields.ur.map(f => if (f.nonEmpty) setElement("ur", f)) else xml.NodeSeq.Empty}
      {if (fields.fulltext.nonEmpty || fields.fulltext != null) setElement("fulltext", fields.fulltext) else xml.NodeSeq.Empty}
      {if (fields.ab.nonEmpty || fields.ab != null) setElement("ab", fields.ab) else xml.NodeSeq.Empty}
      {if (fields.au.nonEmpty || fields.au != null) {fields.au.map(f => if (f.nonEmpty) setElement("au", f))} else xml.NodeSeq.Empty}
      {if (fields.entryDate.nonEmpty || fields.entryDate != null) setElement("entry_date", fields.entryDate) else xml.NodeSeq.Empty}
      {if (fields.da.nonEmpty || fields.da != null) setElement("da", fields.da) else xml.NodeSeq.Empty}
      {if (fields.mj.nonEmpty || fields.mj != null) fields.mj.map(f => if (f.nonEmpty & f != null) setElement("mj", f)) else xml.NodeSeq.Empty}
    </doc>
  }

  private def setElement(name: String, field: String): Elem = <field name={name}>
    {field}
  </field>

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