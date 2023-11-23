package org.bireme.processing.tools.mrw

import com.mongodb.client.{MongoCollection, MongoCursor}
import com.mongodb.client.model.Projections
import org.bson.Document
import org.bson.conversions.Bson

import scala.jdk.CollectionConverters.{CollectionHasAsScala, SeqHasAsJava}

class MongoDbIteratorStr(query: Option[String],
                         coll: MongoCollection[Document],
                         quantity: Int,
                         bufferSize: Int,
                         outputFields: Option[Set[String]]) extends Iterator[Map[String, Array[String]]] {
  private val projections: Option[Bson] = outputFields.map(oflds => Projections.include(oflds.toList.asJava))

  private val iter: MongoCursor[Document] = projections match {
    case Some(projection) =>
      query match {
        case Some(qry) =>
          val filter: Document = Document.parse(qry)
          coll.find(filter).projection(projection).batchSize(bufferSize).limit(quantity).iterator
        case None => coll.find().projection(projection).batchSize(bufferSize).limit(quantity).iterator
      }
    case None =>
      query match {
        case Some(qry) =>
          val filter: Document = Document.parse(qry)
          coll.find(filter).batchSize(bufferSize).limit(quantity).iterator
        case None => coll.find().batchSize(bufferSize).limit(quantity).iterator
      }
  }

  override def hasNext: Boolean = iter.hasNext

  def next(): Map[String, Array[String]] = {
    val doc: Document = iter.next()
    val fields: Iterable[java.util.Map.Entry[String, AnyRef]] = doc.entrySet().asScala

    fields.foldLeft(Map[String, Array[String]]()) {
      case (map1, entry) =>
        entry.getValue match {
          case arr: Array[_] => map1 + (entry.getKey -> arr.map(_.toString))
          case other => map1 + (entry.getKey -> Array(other.toString))
        }
    }
  }
}
