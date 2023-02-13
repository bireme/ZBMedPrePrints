package mongodb

import org.mongodb.scala._
import org.mongodb.scala.model.{Aggregates, Filters}

import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import org.slf4j.{Logger, LoggerFactory}


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
  val coll: MongoCollection[Document] = dbase.getCollection(collection)
  val nameCollection: String = collection.concat("-Normalized")

  val logger: Logger = LoggerFactory.getLogger(classOf[MongoExport])

  def checkLoginMongodb: Boolean = mongoClient.startSession().results().nonEmpty

  def findAll: Seq[Document] = new DocumentObservable(coll.find()).observable.results()

  def createCollection(nameCollection: String): Unit = dbase.createCollection(nameCollection)

  def getCollection(nameCollection: String): Unit = dbase.getCollection(nameCollection)

  def existCollection(nameCollection: String): Boolean = {
    val listCollection: Seq[String] = dbase.listCollectionNames().results()
    listCollection.contains(nameCollection)
  }

  def isIdRepeted(nameField: String, valueField: String): Boolean = {
    coll.aggregate(Seq(Aggregates.filter(Filters.equal(nameField, valueField)))).results().length >= 2
  }

  def insertDocument(doc: String): Unit =  coll.insertOne(Document(doc)).results()

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