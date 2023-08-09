package mrw

import com.mongodb.client.{MongoClient, MongoClients, MongoCollection, MongoDatabase, MongoIterable}
import com.mongodb.client.model.{Filters, FindOneAndReplaceOptions, Indexes}
import org.bson.{BsonValue, Document}
import org.slf4j.{Logger, LoggerFactory}

import java.time.LocalDateTime
import java.util
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.{Success, Try}

case class mdwParameters(database: String,
                         collection: String,
                         clear: Boolean = true,
                         addUpdDate: Boolean = true,
                         idField: Option[String] = None, // if present the document will be upserted otherwise it will be inserted
                         host: Option[String] = None,
                         port: Option[Int] = None,
                         user: Option[String] = None,
                         password: Option[String] = None)

class MongoDbWriter(params: mdwParameters) {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  private val host = params.host.getOrElse("localhost")
  private val port = params.port.getOrElse(27017)

  require(port > 0)

  private val usrPswStr: String = params.user.flatMap {
    usr => params.password.map(psw => s"$usr:$psw@")
  }.getOrElse("")

  private val mongoUri: String = s"mongodb://$usrPswStr$host:$port"
  private val mongoClient: MongoClient = MongoClients.create(mongoUri)
  private val dbase: MongoDatabase = mongoClient.getDatabase(params.database)
  private val coll: MongoCollection[Document] = {
    if (params.clear) {
      dbase.getCollection(params.collection).drop()
      dbase.getCollection(params.collection)
    } else dbase.getCollection(params.collection)
  }

  params.idField match {
    case Some(index_name) => coll.createIndex(Indexes.ascending(index_name))
      val cursor = coll.listIndexes().cursor()
      while (cursor.hasNext) {
        val nameIndex = cursor.next()
        logger.info(s"* Field index defined: ${nameIndex.get("name")}")
      }
    case None => None
  }

  private val upsert: Boolean = params.idField.isDefined
  //private lazy val options: UpdateOptions = new UpdateOptions().upsert(upsert)

  private val dateTime: LocalDateTime = LocalDateTime.now()
  //  private val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss")
  //  private val dateTimeFormatter: String = dateTime.format(formatter)

  def close(): Try[Unit] = Try(mongoClient.close())

  def close(buffer: mutable.Buffer[Map[String,Any]]): Try[Unit] = {
    Try {
      if (buffer.nonEmpty) insertDocuments(buffer.toSeq)
      mongoClient.close()
    }
  }

  def close1(buffer: mutable.Buffer[Map[String, AnyRef]]): Try[Unit] = {
    Try {
      if (buffer.nonEmpty) insertDocuments(buffer.toSeq)
      mongoClient.close()
    }
  }

  def closeStr(buffer: mutable.Buffer[String]): Try[Unit] = {
    Try {
      if (buffer.nonEmpty) insertDocumentsStr(buffer.toSeq)
      mongoClient.close()
    }
  }

  /*def close[T](buffer: mutable.Buffer[T]): Try[Unit] = {
    Try {
      if (buffer.nonEmpty) {
        buffer match {
          case buf: mutable.Buffer[String] => insertDocuments(buf.toSeq)
        }
      mongoClient.close()
    }
  }*/

  def insertDocumentStr(doc: String): Try[String] = {
    Try {
      val document: Document = Document.parse(doc)
      if (params.addUpdDate) {
        updateDateTime(document)
      }
      if (upsert) {
        val newId: String = document.getString(params.idField.get) // Supposing the id field type is String
        val doc: Document = setVersionUpd(document, newId)
        coll.findOneAndReplace(Filters.eq(params.idField.get, newId), doc, new FindOneAndReplaceOptions()
            .upsert(true))
        newId
      } else coll.insertOne(document).getInsertedId.asObjectId().toString
    }
  }

  def insertDocument(doc: Map[String, Any]): Try[String] = {
    Try {
      val document: Document = convertToDocument(doc)
      if (params.addUpdDate) {
        updateDateTime(document)
      }
      if (upsert) {
        val newId: String = document.getString(params.idField.get) // Supposing the id field type is String
        setVersionUpd(document, newId)
        coll.findOneAndReplace(Filters.eq(params.idField.get, newId), document, new FindOneAndReplaceOptions()
          .upsert(true))
        newId
      } else coll.insertOne(document).getInsertedId.asObjectId().toString
    }
  }

  def insertDocument1(doc: Map[String, AnyRef]): Try[String] = {
    Try {
      val document: Document = convertToDocument(doc)
      if (params.addUpdDate) {
        updateDateTime(document)
      }
      if (upsert) {
        val newId: String = document.getString(params.idField.get) // Supposing the id field type is String
        val doc: Document = setVersionUpd(document, newId)
        coll.findOneAndReplace(Filters.eq(params.idField.get, newId), doc, new FindOneAndReplaceOptions()
          .upsert(true))
        newId
      } else coll.insertOne(document).getInsertedId.asObjectId().toString
    }
  }

  def insertDocBuffer(doc: Map[String, Any],
                      buffer: mutable.Buffer[Map[String, Any]],
                      maxSize: Int = 1000): Try[Unit] = {
    buffer.addOne(doc)
    if (buffer.size == maxSize) {
      insertDocuments(buffer.toSeq)
      buffer.clear()
      Success(())
    } else Success(())
  }

  def insertDocBuffer1(doc: Map[String, AnyRef],
                       buffer: mutable.Buffer[Map[String, AnyRef]],
                       maxSize: Int = 1000): Try[Unit] = {
    buffer.addOne(doc)
    if (buffer.size == maxSize) {
      insertDocuments(buffer.toSeq)
      buffer.clear()
      Success(())
    } else Success(())
  }

  def insertDocBufferStr(doc: String,
                         buffer: mutable.Buffer[String],
                         maxSize: Int = 1000): Try[Unit] = {
    buffer.addOne(doc)
    if (buffer.size == maxSize) {
      print("+++ writing documents to mongoDB...")
      val result: Try[Seq[String]] = insertDocumentsStr(buffer.toSeq)
      println(" OK")
      buffer.clear()
      result.map(_ => ())
    } else Success(())
  }

  def insertDocumentsStr(docs: Seq[String]): Try[Seq[String]] = { // Do not do upsert
    Try {
      val documents1: Seq[Document] = docs.map(Document.parse)
      documents1.foreach { document =>
        if (params.addUpdDate) {
          updateDateTime(document)
        }
      }
      val documents: util.List[Document] = documents1.asJava
      val insertedIds: util.Collection[BsonValue] = coll.insertMany(documents).getInsertedIds.values()
      insertedIds.asScala.toSeq.map {
        elem =>
          if (elem.isObjectId) elem.asObjectId().toString
          else elem.toString
      }
    }
  }

  def insertDocuments(docs: Seq[Map[String, Any]]): Try[Seq[String]] = { // Do not do upsert
    Try {
      val documents1: Seq[Document] = docs.map(convertToDocument)
      documents1.foreach { document =>
        if (params.addUpdDate) {
          updateDateTime(document)
        }
      }
      val documents: util.List[Document] = documents1.asJava
      val insertedIds: util.Collection[BsonValue] = coll.insertMany(documents).getInsertedIds.values()

      insertedIds.asScala.toSeq.map(_.asObjectId().toString)
    }
  }

  def insertDocuments1(docs: Seq[Map[String, AnyRef]]): Try[Seq[String]] = { // Do not do upsert
    Try {
      val documents1: Seq[Document] = docs.map(convertToDocument)
      documents1.foreach { document =>
        if (params.addUpdDate) {
          updateDateTime(document)
        }
      }
      val documents: util.List[Document] = documents1.asJava
      val insertedIds: util.Collection[BsonValue] = coll.insertMany(documents).getInsertedIds.values()

      insertedIds.asScala.toSeq.map(_.asObjectId().toString)
    }
  }

  def insertDocumentNormalized(docs: List[String]): Unit = {
    val collNomalized: MongoCollection[Document] = dbase.getCollection(params.collection)
    collNomalized.insertMany(docs.map(Document.parse).asJava)
  }

  def createCollection(nameCollection: String): Unit = {
    dbase.createCollection(nameCollection)
  }

  def existCollection(nameCollection: String): Boolean = {
    val listCollection: MongoIterable[String] = dbase.listCollectionNames()
    listCollection.asScala.exists(_.contains(nameCollection))
  }


  private def updateDateTime(doc: Document) = {

    val nameFieldMetadata: String = "_metadata"
    val metadataDocument = doc.get(nameFieldMetadata, classOf[Document])

    if (metadataDocument != null) {
      doc.get(nameFieldMetadata, classOf[Document]).remove("_updd")
      doc.get(nameFieldMetadata, classOf[Document]).append("_updd", dateTime)
    } else {
      val newMetadataDocument = new Document("_updd", dateTime)
      doc.append(nameFieldMetadata, newMetadataDocument)
    }
  }

  def countDocuments(): Int = coll.countDocuments().toInt

  private def convertToDocument(document: Map[String, Any]): Document = {
    val d1 = document.foldLeft(new Document()) {
      case (doc, (k,v)) =>
        v match {
          case ref: AnyRef => doc.append(k, convertRef(ref))
          case any => doc.append(k, any)
        }
    }
    d1
  }

  private def setVersionUpd(document: Document, newId: String): Document = {

    val nameFieldMetadata: String = "_metadata"
    val nameFieldVersion: String = "_version"
    val docWithMetadata: Document = document

    if (docWithMetadata.get(nameFieldMetadata, classOf[Document]).containsKey(nameFieldVersion)) {
      val valueVersion: Int = docWithMetadata.getInteger(nameFieldVersion).toInt
      docWithMetadata.remove(nameFieldVersion)
      docWithMetadata.append(nameFieldVersion, valueVersion.+(1))
    } else {
      if (coll.find(Filters.eq(params.idField.get, newId)).first() != null) {
        val valueVersionNew = coll.find(Filters.eq(params.idField.get, newId)).first().get(nameFieldMetadata,
          classOf[Document]).getInteger(nameFieldVersion).toInt
        docWithMetadata.get(nameFieldMetadata, classOf[Document]).append(nameFieldVersion, valueVersionNew.+(1))
      } else {
        docWithMetadata.get(nameFieldMetadata, classOf[Document]).append(nameFieldVersion, 1)
        document
      }
    }
  }

  def convertToDocument1(document: Map[String, AnyRef]): Document = {
    val d1 = document.foldLeft(new Document()) {
      case (doc, (k, v)) =>
        doc.append(k, convertRef(v))
    }
    d1
  }

  private def convertRef(ref: Any): Any = {
    val newRef = ref match {
      case arr: Array[Any] =>
        arr.map(convertRef).toList.asJava
      case lst: List[Any] =>
        lst.map(convertRef).asJava
      case map: Map[String, Any] =>
        convertToDocument(map)
      case other =>
        other
    }
    newRef
  }

  private def convertRef1(ref: AnyRef): AnyRef = {
    val newRef = ref match {
      case arr: Array[_] =>
        arr.map(convertRef).toList.asJava
      case lst: List[_] =>
        lst.map(convertRef).asJava
      case map: Map[String, _] =>
        convertToDocument(map)
      case other =>
        other
    }
    newRef
  }
}

object MongoDbWriter extends App {
  private val mdrp = mdrParameters("ZBMed", "preprints_full", quantity=Some(10)/*, outputFields=Some(Set("Id", "Descritor Português","Código Hierárquico"))*/)
  private val mdwp = mdwParameters("ZBMed", "test"/*, idField=Some("Id")*/)
  private val mReader = new MongoDbReader(mdrp)
  private val mWriter = new MongoDbWriter(mdwp)
  private val llist = mReader.lazyList()
  //private val buffer = mutable.Buffer[Map[String, AnyRef]]()

  llist.foreach {
    elem =>
      elem.foreach {
        doc => {
          val id: String = doc.getOrElse("Id", "?").toString
          println(s"id=$id")
          //mWriter.insertDocBuffer(doc, buffer)
          mWriter.insertDocument(doc)
        }
      }
  }
  mReader.close()
  mWriter.close()
}
