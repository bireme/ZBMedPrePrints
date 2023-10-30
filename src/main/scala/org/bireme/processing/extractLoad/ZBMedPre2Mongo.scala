package org.bireme.processing.extractLoad

import ch.qos.logback.classic.ClassicConstants
import org.bireme.processing.tools.mrw.{MongoDBTools, MongoDbReader, MongoDbWriter, mdrParameters, mdwParameters}
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json._

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.{LocalDate, Year}
import java.time.format.DateTimeFormatter
import java.util.Date
import scala.annotation.tailrec
import scala.collection.immutable.TreeMap
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

object ZBMedPre2Mongo extends App {

  System.setProperty(ClassicConstants.CONFIG_FILE_PROPERTY, "./src/main/resources/logback.xml")
  val logger: Logger = LoggerFactory.getLogger(getClass)
  private def usage(): Unit = {
    System.err.println("Import preprints articles from ZBMed API into a MongoDB collection")
    System.err.println("usage: ZBMedPre2Mongo <options>")
    System.err.println("options:")
    System.err.println("\t-database:<name>       - MongoDB database name")
    System.err.println("\t-collection:<name>     - MongoDB database collection name")
    System.err.println("\t[-decsDatabase:<name>] - MongoDB DeCS database name. Default is 'DECS'")
    System.err.println("\t[-decsCollection:<name>]  - MongoDB DeCS database collection name. Default is the current year")
    System.err.println("\t[(-fromDate:<yyyy-mm-dd> | -daysBefore:<days>)] - initial date or number of days before today")
    System.err.println("\t[-toDate:<yyyy-mm-dd>] - end date. Default is today")
    System.err.println("\t[-quantity:<num>]      - number of preprints to import. Default is unlimited")
    System.err.println("\t[-excludeSources:<src1>,...,<srcn>] - exclude documentos whose sources is here listed")
    System.err.println("\t[-host:<name>]         - MongoDB server name. Default value is 'localhost'")
    System.err.println("\t[-port:<number>]       - MongoDB server port number. Default value is 27017")
    System.err.println("\t[-user:<name>])        - MongoDB user name. Default is not to use an user")
    System.err.println("\t[-password:<pwd>]      - MongoDB user password. Default is not to use an password")
    System.err.println("\t[--reset]              - initializes the MongoDB collection if it is not empty")
    System.err.println("\t[--importByMonth]      - if present, the system will load documents month by month")
    System.err.println("\t[--checkRepeatable]    - if present, the system will check if the document was not already inserted (id check)")
    System.exit(1)
  }

  private case class WriteParameters(database: String,
                                     collection: String,
                                     fromDate: LocalDate,
                                     toDate: LocalDate,
                                     excludeSources: Seq[String],
                                     quantity: Option[Int],
                                     host: Option[String],
                                     port: Option[Int],
                                     user: Option[String],
                                     password: Option[String],
                                     reset: Boolean,
                                     importByMonth: Boolean,
                                     checkRepeatable: Boolean
                                    )

  //def main(args: Array[String]): Unit =
  if (args.length < 2) usage()

  private val parameters: Map[String, String] = args.foldLeft[Map[String, String]](Map()) {
    case (map, par) =>
      val split = par.split(" *: *", 2)
      if (split.size == 1) map + ((split(0).substring(2), ""))
      else map + ((split(0).substring(1), split(1)))
  }

  if (!Set("database", "collection").forall(parameters.contains)) usage()

  private val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  val database: String = parameters.getOrElse("decsDatabase", "DECS")
  val host: Option[String] = parameters.get("host")
  val port: Option[Int] = parameters.get("port").map(_.toInt)
  val user: Option[String] = parameters.get("user")
  val password: Option[String] = parameters.get("password")
  val collection: String = parameters.getOrElse("decsCollection",
                                                getDecsCollection(database, host, port, user, password))
  private val rParamDECS = mdrParameters(
    database = database,
    host = host,
    port = port,
    user = user,
    password = password,
    collection = collection,
    bufferSize = Some(1),  // DeCS one document per id
    outputFields = Some(Set("Descritor Inglês", "Descritor Espanhol", "Descritor Português", "Descritor Francês", "Mfn"))
  )

  private val wParam = WriteParameters(
    database = parameters("database"),
    collection = parameters("collection"),
    fromDate = parameters.get("fromDate") match {
      case Some(fDate) => LocalDate.parse(fDate, formatter)
      case None => parameters.get("daysBefore") match {
        case Some(dBefore) => LocalDate.now().atStartOfDay().minusDays(dBefore.toLong - 1).toLocalDate
        case None => LocalDate.now().minusYears(10)
      }
    },
    toDate = parameters.get("toDate") match {
      case Some(toD) => LocalDate.parse(toD, formatter)
      case None => LocalDate.now()
    },
    excludeSources = parameters.getOrElse("excludeSources", "").trim.split(" *, *").toSeq,
    quantity = parameters.get("quantity").map(_.toInt),
    host = parameters.get("host"),
    port = parameters.get("port").map(_.toInt),
    user = parameters.get("user"),
    password = parameters.get("password"),
    reset = parameters.contains("reset"),
    importByMonth = parameters.contains("importByMonth"),
    checkRepeatable = parameters.contains("checkRepeatable")
  )

  private val rParamTmp = mdrParameters(
    database = parameters("database"),
    collection = parameters("collection"),
    host = parameters.get("host"),
    port = parameters.get("port").map(_.toInt),
    user = parameters.get("user"),
    password = parameters.get("password"),
    bufferSize = None,
    outputFields = None
  )

  importPreprints(rParamDECS, rParamTmp, wParam, formatter) match {
    case Success(_) =>
      logger.info("Importing documents finished!\n")
      System.exit(0)
    case Failure(exception) =>
      logger.error(s"Importing documents ERROR: ${exception.getMessage}")
      System.exit(1)
  }

  private def getDecsCollection(database: String,
                                host: Option[String],
                                port: Option[Int],
                                user: Option[String],
                                password: Option[String]) : String = {
    // Supposing the collections' names are only years (2023, 2024, etc)
    MongoDBTools.listCollections(database, host, port, user, password).map {
      set => set.map(_.toInt)
    }.map(_.max).map(_.toString) match {
      case Success(year) => year
      case Failure(_) => Year.now().getValue.toString   // If some problem occurred, take the current year
    }
  }

  private def importPreprints(rParamDECS: mdrParameters,
                              rParamTmp: mdrParameters,
                              wParams: WriteParameters,
                              formatter: DateTimeFormatter): Try[Unit] = {
    Try {
      val startDate: Date = new Date()
      logger.info(s"Import started - ZBMed preprints $startDate")

      val mongoReaderDECS: MongoDbReader = new MongoDbReader(rParamDECS)
      val mongoReader: MongoDbReader = new MongoDbReader(rParamTmp)

      if (mongoReader.collectionExists(wParams.collection)){
        val qtdColl: Int = mongoReader.countDocuments(wParams.collection)

        val writerParameter: mdwParameters = mdwParameters(wParams.database, wParams.collection.concat("_after"),
          wParams.reset, addUpdDate = true, idField = Some("id"), wParams.host, wParams.port, wParams.user, wParams.password)
        val mongoWriter: MongoDbWriter = new MongoDbWriter(writerParameter)

        importZBMed(wParams, formatter, mongoReaderDECS, mongoWriter)
        val qtdCollAfter: Int = mongoReader.countDocuments(wParams.collection.concat("_after"))
        mongoWriter.close()

        if (qtdColl > qtdCollAfter) {
          flushAndEnd(wParams, startDate, mongoReader, mongoReaderDECS)
        } else if (qtdColl < qtdCollAfter) {
          mongoReader.dropColl(wParams.collection)
          mongoReader.renameColl(oldNameColl = wParams.collection.concat("_after"), newNameColl = wParams.collection)
        } else mongoReader.dropColl(wParams.collection.concat("_after"))
      } else {

        val writerParameter: mdwParameters = mdwParameters(wParams.database, wParams.collection,
          wParams.reset, addUpdDate = true, idField = Some("id"), wParams.host, wParams.port, wParams.user, wParams.password)

        val mongoWriter: MongoDbWriter = new MongoDbWriter(writerParameter)
        importZBMed(wParams, formatter, mongoReaderDECS, mongoWriter)
        mongoWriter.close()
      }
      mongoReaderDECS.close()
      mongoReader.close()

      logger.info(timeAtProcessing(startDate))
    }
  }

  private def flushAndEnd(wParams: WriteParameters, startDate: Date, mongoReader: MongoDbReader, mongoReaderDECS: MongoDbReader): Unit = {

    mongoReader.dropColl(wParams.collection.concat("_after"))
    mongoReader.close()
    mongoReaderDECS.close()
    logger.error(s"Importing documents with a quantity lower than the previous processing!")
    logger.info(timeAtProcessing(startDate))
    System.exit(1)
  }

  private def importZBMed(wParams: WriteParameters, formatter: DateTimeFormatter, mongoReader: MongoDbReader, mongoWriter: MongoDbWriter): Unit = {

    val fromDate: String = wParams.fromDate.format(formatter)
    val toDate: String = wParams.toDate.format(formatter)
    val history: Option[mutable.Map[String, (LocalDate, String)]] = if (wParams.checkRepeatable)
      Some(mutable.Map[String, (LocalDate, String)]()) else None
    if (wParams.importByMonth) importPreprintsByMonth(mongoReader, mongoWriter, LocalDate.parse(fromDate),
      LocalDate.parse(toDate), formatter, wParams.excludeSources, history)
    else importPreprints(mongoReader, mongoWriter, LocalDate.parse(fromDate), LocalDate.parse(toDate), 0,
      wParams.quantity, wParams.excludeSources, total = 0, history)
  }

  @tailrec
  private def importPreprints(mongoImport: MongoDbReader,
                              mongoExport: MongoDbWriter,
                              fromDate: LocalDate,
                              toDate: LocalDate,
                              from: Int,
                              quantity: Option[Int],
                              excludeSources: Seq[String],
                              total: Int = 0,
                              history: Option[mutable.Map[String, (LocalDate, String)]]): Unit = {
    if (quantity.isEmpty || quantity.get > 0) {
      val quant: Int = quantity.map(Math.min(_, 1000)).getOrElse(1000)

      val (imported, relation, total1) = callApi(from, quant, fromDate, toDate, formatter).
        flatMap(response => insertDocuments(mongoImport, mongoExport, response, excludeSources, fromDate, history)) match {
        case Success((docs, relat, tot)) => (docs.length, relat, tot)
        case Failure(exception) =>
          logger.error(s"--- importPreprints error. fromDate=$fromDate toDate=$toDate error=${exception.getMessage}")
          (0, "", 0)
      }

      val nfrom: Int = from + quant
      val nquantity: Option[Int] = quantity match {
        case Some(qtt) => Some(qtt - quant)
        case None => if (relation.equals("eq")) Some(total1 - (from + quant))
        else {
          if (imported > 0) None
          else Some(0)
        }
      }

      logger.info(s"+++ importPreprints from=${from + quant} step_imported=$imported total_imported=${total + imported} " +
        s"remaining=${nquantity.getOrElse(0)}")
      importPreprints(mongoImport, mongoExport, fromDate, toDate, nfrom, nquantity, excludeSources, total + imported, history)
    }
  }

  @tailrec
  private def importPreprintsByMonth(mongoImport: MongoDbReader,
                                     mongoExport: MongoDbWriter,
                                     fromDate: LocalDate,
                                     toDate: LocalDate,
                                     formatter: DateTimeFormatter,
                                     excludeSources: Seq[String],
                                     history: Option[mutable.Map[String, (LocalDate,String)]],
                                     totalImported: Int = 0): Int = {
    val tDate1: LocalDate = fromDate.plusMonths(1).minusDays(1)
    val tDate2: LocalDate = if (toDate.isBefore(tDate1)) toDate else tDate1
    if (fromDate.isBefore(tDate2)) {
      val (imported, _, _) = callApi(from=0, size=59999, fromDate, tDate2, formatter).
        flatMap(response => insertDocuments(mongoImport, mongoExport, response, excludeSources, fromDate, history)) match {
        case Success((docs, relat, tot)) => (docs.length, relat, tot)
        case Failure(exception) =>
          logger.error(s"--- importPreprints error. fromDate=$fromDate toDate=$toDate error=${exception.getMessage}")
          (0, "", 0)
      }
      val updatedTotalImported = totalImported + imported
      logger.info(s"+++ importPreprints fromDate=$fromDate toDate=$tDate2 total_imported=$imported")

      val fDate =  tDate2.plusDays(1)
      importPreprintsByMonth(mongoImport, mongoExport, fDate, toDate, formatter, excludeSources, history, updatedTotalImported)
    } else {
      logger.info(s"Total imported: $totalImported")
      totalImported
    }
  }

  private def callApi(from: Int,
                      size: Int,
                      fromDate: LocalDate,
                      toDate: LocalDate,
                      formatter: DateTimeFormatter): Try[HttpResponse[String]] = {
    Try {
      val ZBMedUrl: String = "https://preview.zbmed.de/api/documents/"
      val fromDateStr: String = formatter.format(fromDate)
      val toDateStr: String = formatter.format(toDate)
      val parameters: String = s"""{"size":$size,"from":$from,"query":{"bool":{"must":[{"range":{"date":{"gte":"$fromDateStr","lte":"$toDateStr"}}}]}},"sort":[{"_score":{"order":"asc"}}],"track_total_hits":true}"""
      val client = HttpClient.newHttpClient()
      val request: HttpRequest = HttpRequest.newBuilder().uri(URI.create(ZBMedUrl))
        .POST(HttpRequest.BodyPublishers.ofString(parameters)).header("Accept", "application/json")
        .header("Content-Type", "application/json")
        .build()
      client.send(request, HttpResponse.BodyHandlers.ofString())
    }
  }

  private def insertDocuments(mongoImport: MongoDbReader,
                              mongoExport: MongoDbWriter,
                              response: HttpResponse[String],
                              excludeSources: Seq[String],
                              fromDate: LocalDate,
                              history: Option[mutable.Map[String, (LocalDate,String)]]):
                                                                        Try[(Seq[Map[String, AnyRef]], String, Int)] = {
    response.statusCode() match {
      case 200 =>
        val body: String = response.body()
        if (body.nonEmpty) {
          getDocuments(mongoImport, body, excludeSources).map {
            case (docs, relation, total) =>
              history match {
                case Some(hist) =>
                  docs.foreach {
                    doc =>
                      val id: String = doc.getOrElse("id", "").toString
                      val title: String = doc.getOrElse("title", "").toString
                      hist.get(id) match {
                        case Some((date, otherTitle)) =>
                          logger.error(s"--- insertDocuments error. fromDate=$fromDate id=$id otherDate=$date title=}$title " +
                            s"otherTitle=$otherTitle")
                        case None =>
                          hist.addOne(id, (fromDate, title))
                          mongoExport.insertDocument(doc) match {
                            case Success(_) => ()
                            case Failure(exception) =>
                              logger.error(s"insertion error. id=$id reason:$exception")
                          }
                      }
                  }
                case None => if (docs.nonEmpty) mongoExport.insertDocuments(docs)
              }
              (docs, relation, total)
          }
        } else Failure(new Exception(s"ZBMED ERROR: http communication error. ${response.request().toString}"))
      case other => Failure(new Exception(s"ZBMED ERROR: http communication error. Code: $other}"))
    }
  }

  private def getDocuments(mongoImport: MongoDbReader,
                           docs: String,
                           excludeSources: Seq[String]): Try[(Seq[Map[String, AnyRef]], String, Int)] = {
    Try {
      val json: JsValue = Json.parse(docs)

      val docsSeq: Seq[Map[String, AnyRef]] = Option(json("content")) match {
        case Some(content) =>
          content match {
            case cont: JsArray =>
              val docsSeq: Seq[JsObject] = fixFields(mongoImport, cont.value.map(_.as[JsObject]).toSeq, excludeSources).get
              val docsMap: Seq[Map[String, AnyRef]] = docsSeq.map(convertJsObject)
              docsMap
            case _ => throw new IllegalArgumentException(s"missing 'content' element: $docs")
          }
        case None => throw new IllegalArgumentException(s"missing 'content' element: $docs")
      }
      val (total, relation) = Option(json("totalElements")) match {
        case Some(content) => content match {
          case cont: JsObject =>
            val relation: String = cont.value.getOrElse("relation", JsString("")).as[String]
            val total: Int = cont.value.getOrElse("value", JsNumber(0)).as[Int]
            (total, relation)
          case _ => throw new IllegalArgumentException(s"missing 'totalElements' element: $docs")
        }
        case None => throw new IllegalArgumentException(s"missing 'totalElements' element: $docs")
      }
      (docsSeq, relation, total)
    }
  }

  private def fixFields(mongoImport: MongoDbReader,
                        docs: Seq[JsObject],
                        excludeSources: Seq[String]): Try[Seq[JsObject]] = {
    filterSources(docs, excludeSources).flatMap(fixAuthors).flatMap(docs => fixMESH(docs, mongoImport))
  }

  private def filterSources(docs: Seq[JsObject],
                            excludeSources: Seq[String]): Try[Seq[JsObject]] = {
    Try {
      docs.filterNot {
        doc =>
          val source: String = doc("source").as[JsString].value
          excludeSources.contains(source)
      }
    }
  }

  private def fixAuthors(docs: Seq[JsObject]): Try[Seq[JsObject]] = Try(docs.map(fixAuthors))

  private def fixAuthors(doc: JsObject): JsObject = {
    val authors: JsArray = doc("authors").as[JsArray]
    val authors1: JsArray = JsArray(authors.value.map {
      author => JsObject(Map("author_inst" -> JsString(""), "author_name" -> author))
    })
    doc - "authors" + ("rel_authors" -> authors1)
  }

  private def fixMESH(docs: Seq[JsObject],
                      mongoImport: MongoDbReader): Try[Seq[JsObject]] =
    Try(docs.map(doc => fixMESH(doc, mongoImport)))

  private def fixMESH(doc: JsObject,
                      mongoImport: MongoDbReader): JsObject = {
    val nFields: Seq[(String, JsValue)] = doc.fields.foldLeft(Seq[(String, JsValue)]()) {
      case (fields, (fieldName, value)) =>
        if ("all_annotations".equals(fieldName)) {
          val nValue: Seq[JsValue] = value.as[JsArray].value.foldLeft(Seq[JsValue]()) {
            case (seq, elem) =>
              val content: String = elem.as[JsString].value
              getId(content) match {
                case Some((tClass, tId)) =>
                  val concept: JsString = JsString(content)
                  val isMesh: Boolean = concept.value.contains("MESHD")
                  val sObj: Seq[(String, JsString)] =
                    Seq("class" -> JsString(tClass), "concept" -> concept, "id" -> JsString(tId))
                  val mObj: Seq[(String, JsString)] = {
                    if (isMesh) {
                      getMeshFields(tId, mongoImport) match {
                        case Some(mFlds) => sObj ++ mFlds
                        case None => sObj
                      }
                    } else sObj
                  }
                  val y: Seq[JsValue] = seq.appended(JsObject(mObj))
                  y
                case None => seq   // For now, ignoring the error
              }
          }
          fields.appended(fieldName -> JsArray(nValue))
        } else fields.appended(fieldName -> value)
    }
    JsObject(nFields)
  }

  private def getId(str: String): Option[(String,String)] = {
    "^([A-Z]+):".r.findFirstMatchIn(str) match {
      case Some(mat) =>
        val tClass1 = mat.group(1)
        val tId1 = tClass1 match {
          case "MESHD" => "/([A-Z]?\\d+)$".r.findFirstMatchIn(str) match {
            case Some(mat1) => mat1.group(1)
            case None => ""
          }
          case "HGNC" => ":(\\d+)$".r.findFirstMatchIn(str) match {
            case Some(mat1) => mat1.group(1)
            case None => ""
          }
          case "SC2V" => "/([A-Z\\d]+)$".r.findFirstMatchIn(str) match {
            case Some(mat1) => mat1.group(1)
            case None => ""
          }
          case other =>
            logger.warn(s"unknown thesaurus [$other]")
            ""
        }
        Some((tClass1, tId1))
      case None =>
        "/MESH/([A-Z]?\\d+)$".r.findFirstMatchIn(str) match {
          case Some(mat1) => Some(("MESHD", mat1.group(1)))
          case None => "/([^/]+?)/([^$]+)$".r.findFirstMatchIn(str) match {
            case Some(mat1) => Some((mat1.group(1), mat1.group(2)))
            case None => None
          }
        }
    }
  }

  private def getMeshFields(id: String,
                            mongoImport: MongoDbReader): Option[Seq[(String, JsString)]] = {
    mongoImport.iterator(Some(s"{\"Id\":\"$id\"}")) match {
      case Success(iter) =>
        iter.toSeq.headOption match {
          case Some(doc) =>
            val map: Map[String, JsString] = doc.foldLeft(TreeMap[String, JsString]()) {
              case (map1, (k, v)) =>
                k match {
                  case "Descritor Inglês" => map1 + ("descriptor_en" -> JsString(v.toString))
                  case "Descritor Espanhol" => map1 + ("descriptor_es" -> JsString(v.toString))
                  case "Descritor Português" => map1 + ("descriptor_pt" -> JsString(v.toString))
                  case "Descritor Francês" => map1 + ("descriptor_fr" -> JsString(v.toString))
                  case "Mfn" => map1 + ("mfn" -> JsString(v.toString.toInt.toString))  // to remove leading zeros
                  case "_id" => map1
                  case other => map1 + (other -> JsString(v.toString))
                }
            }
            Some(map.toSeq)
          case None => None
        }
      case Failure(exception) =>
        logger.error(s"getMeshFields ERROR: id=$id status=$exception")
        None
    }
  }

  private def convertJsObject(obj: JsObject): Map[String, AnyRef] = {
    TreeMap.from(obj.fields.map(x => (x._1, convertJsValue(x._2))))
  }

  private def convertJsValue(value:JsValue): AnyRef = {
    value match {
      case obj: JsObject => convertJsObject(obj)
      case arr: JsArray => arr.value.map(convertJsValue).toArray
      case bool: JsBoolean => bool.value.toString
      case number: JsNumber => number.toString()
      case str: JsString => str.value
      case other => other.toString()
    }
  }

  def timeAtProcessing(startDate: Date): String = {
    val endDate: Date = new Date()
    val elapsedTime: Long = (endDate.getTime - startDate.getTime) / 1000
    val minutes: Long = elapsedTime / 60
    val seconds: Long = elapsedTime % 60
    s"Processing time: ${minutes}min e ${seconds}s\n"
  }
}
