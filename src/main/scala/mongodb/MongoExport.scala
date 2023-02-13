package mongodb

import com.google.gson.Gson
import org.mongodb.scala._

import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import org.mongodb.scala.model.{Aggregates, Filters}
import org.slf4j.{Logger, LoggerFactory}
import ppzbmedxml.ZBMedpp_doc


class MongoExport(database: String,
                  collection: String,
                  host: Option[String] = None,
                  port: Option[Int] = None,
                  user: Option[String] = None,
                  password: Option[String] = None) {
  require((user.isEmpty && password.isEmpty) || (user.nonEmpty && password.nonEmpty))

  private val hostStr: String = host.getOrElse("localhost")
  private val portStr: String = port.getOrElse(27017).toString
  private val usrPswStr: String = user match {
    case Some(usr)
    => s"$usr:${password.get}@"
    case None => ""
  }

  private val mongoUri: String = s"mongodb://$usrPswStr$hostStr:$portStr"
  private val mongoClient: MongoClient = MongoClient(mongoUri)
  private val dbase: MongoDatabase = mongoClient.getDatabase(database)
  private val coll: MongoCollection[Document] = dbase.getCollection(collection)

  val logger: Logger = LoggerFactory.getLogger(classOf[MongoExport])

  def checkLoginMongodb: Boolean = mongoClient.startSession().results().nonEmpty

  def findAll: Seq[Document] = new DocumentObservable(coll.find()).observable.results()

  def insertDocumentNormalized(doc: ZBMedpp_doc): Unit = {

    val nameCollection: String = collection.concat("-Standard")
    val docJson = new Gson().toJson(doc)

    if (!existsCollectionNormalized(nameCollection)){
      dbase.createCollection(nameCollection)
      logger.info(s"Collection created: $nameCollection")
      val collNormalized: MongoCollection[Document] = dbase.getCollection(nameCollection)
      logger.info(s"Inserting normalized ZBMed documents into the collection: $nameCollection")
      collNormalized.insertOne(Document(docJson)).results()
    } else {
      val collNormalized: MongoCollection[Document] = dbase.getCollection(nameCollection)
      val isRepeted = collNormalized.aggregate(Seq(Aggregates.filter(Filters.equal("id", doc.id)))).results().length >= 2
      if (!isRepeted){
        collNormalized.insertOne(Document(docJson)).results()
      }
    }
  }

  private def existsCollectionNormalized(nameCollection: String): Boolean = {
    val listCollection = dbase.listCollectionNames().results()
    listCollection.contains(nameCollection)
  }

  implicit class DocumentObservable(val observable: Observable[Document]) extends ImplicitObservable[Document] {
    override val converter: Document => String = doc => doc.toJson()
  }

  implicit class GenericObservable[C](val observable: Observable[C]) extends ImplicitObservable[C] {
    override val converter: C => String = doc => Option(doc).map(_.toString).getOrElse("")
  }

  trait ImplicitObservable[C] {
    val observable: Observable[C]
    val converter: C => String

    def results(): Seq[C] = Await.result(observable.toFuture(), Duration(120, TimeUnit.SECONDS))
  }
}