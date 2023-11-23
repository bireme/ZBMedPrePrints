package org.bireme.processing.tools.mrw

import com.mongodb.client.{MongoClient, MongoClients, MongoDatabase}

import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.util.Try

object MongoDbTools {
  def listCollections(database: String,
                      host: Option[String] = None,
                      port: Option[Int] = None,
                      user: Option[String] = None,
                      password: Option[String] = None): Try[Set[String]] = {
    Try {
      val host2 = host.getOrElse("localhost")
      val port2 = port.getOrElse(27017)
      val usrPswStr: String = user.flatMap {
        usr => password.map(psw => s"$usr:$psw@")
      }.getOrElse("")

      val mongoUri: String = s"mongodb://$usrPswStr$host2:$port2"
      val mongoClient: MongoClient = MongoClients.create(mongoUri)
      val dbase: MongoDatabase = mongoClient.getDatabase(database)

      val collections: Set[String] = dbase.listCollectionNames().asScala.toSet

      mongoClient.close()
      collections
    }
  }
}
