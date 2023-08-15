package mrw

import com.mongodb.client.{MongoClient, MongoClients, MongoCollection, MongoDatabase}
import com.mongodb.client.model.{Filters, Indexes}
import org.bson.Document
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Try

case class mdrParameters(database: String,
                         collection: String,
                         host: Option[String] = None,
                         port: Option[Int] = None,
                         user: Option[String] = None,
                         password: Option[String] = None,
                         bufferSize: Option[Int] = None,
                         quantity: Option[Int] = None,
                         outputFields: Option[Set[String]] = None,
                         indexName: Option[String] = None)

class MongoDbReader(params: mdrParameters) {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  private val host = params.host.getOrElse("localhost")
  private val port = params.port.getOrElse(27017)
  private val bufferSize = params.bufferSize.getOrElse(1000)
  private val quantity = params.quantity.getOrElse(Integer.MAX_VALUE)

  require(port > 0)
  require(bufferSize > 0)
  require(quantity > 0)

  private val usrPswStr: String = params.user.flatMap {
    usr => params.password.map(psw => s"$usr:$psw@")
  }.getOrElse("")

  private val mongoUri: String = s"mongodb://$usrPswStr$host:$port"
  private val mongoClient: MongoClient = MongoClients.create(mongoUri)
  private val dbase: MongoDatabase = mongoClient.getDatabase(params.database)
  private val coll: MongoCollection[Document] = dbase.getCollection(params.collection)


  params.indexName match {
    case Some(index_name) => coll.createIndex(Indexes.ascending(index_name))
      val cursor = coll.listIndexes().cursor()
      while (cursor.hasNext){
        val nameIndex = cursor.next()
        logger.info(s"-Field index defined: ${nameIndex.get("name")}")
      }
    case None => None
  }

  def close(): Try[Unit] = Try(mongoClient.close())

  def findDocumentByKeyValuePair(chave: String, valor: AnyRef): Option[Document] = {
    val filtro = Filters.eq(chave, valor)
    val documento = coll.find(filtro).first()
    Option(documento)
  }

  def collectionExists(nameColl: String = params.collection): Boolean = {
    Try {
      val collIterator = dbase.listCollectionNames().iterator()
      val collSeq = Iterator.continually(collIterator).takeWhile(_.hasNext).map(_.next()).toSeq
      collSeq.contains(nameColl)
    }.getOrElse(false)
  }

  def countDocuments(): Int = coll.countDocuments().toInt

  def iterator(query: Option[String] = None): Try[Iterator[Map[String, AnyRef]]] =
    Try (new MongoIterator(query, coll, quantity, bufferSize, params.outputFields))

  def iteratorStr(query: Option[String] = None): Try[Iterator[Map[String, Array[String]]]] =
    Try(new MongoIteratorStr(query, coll, quantity, bufferSize, params.outputFields))

  def lazyList(query: Option[String] = None): Try[LazyList[Map[String, AnyRef]]] = {
    iterator(query).map {
      iter =>
        def loop(it: Iterator[Map[String, AnyRef]]): LazyList[Map[String, AnyRef]] =
          if (it.hasNext) it.next() #:: loop(it) else LazyList.empty
        loop(iter)
    }
  }

  def lazyListStr(query: Option[String] = None): Try[LazyList[Map[String, Array[String]]]] = {
    iteratorStr(query).map {
      iter =>
        def loop(it: Iterator[Map[String, Array[String]]]): LazyList[Map[String, Array[String]]] =
          if (it.hasNext) it.next() #:: loop(it) else LazyList.empty

        loop(iter)
    }
  }
}

object MongoDbReader extends App {
  private val mdrp = mdrParameters("decs", "2022", quantity=Some(10), outputFields=Some(Set("Id", "Descritor Português","Código Hierárquico")))
  private val mRead = new MongoDbReader(mdrp)
  //private val iter: Try[Iterator[Map[String, AnyRef]]] = mRead.iterator()
  //private val iterStr: Try[Iterator[Map[String, Array[String]]]] = mRead.iteratorStr()
  private val llist = mRead.lazyList()
  //private val llistStr = mRead.lazyListStr()

  llist.foreach {
    elem =>
      elem.foreach {
        doc =>
          println("\n-------------------------------------------------------")
          doc.foreach {
            case (key, ref) => ref match {
              case arr:Array[_] => println(s"[$key]:{${arr.mkString(",")}}")
              case other => println(s"[$key]:[$other]")
            }
          }
      }
  }
  mRead.close()
}
