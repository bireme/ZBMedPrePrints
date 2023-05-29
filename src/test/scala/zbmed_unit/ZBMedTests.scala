package zbmed_unit

import ch.qos.logback.classic.ClassicConstants
import mongodb_unit.MongoTests
import org.mongodb.scala.Document
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import operation.ZBMedppToXml


class ZBMedTests extends AnyFunSuite with BeforeAndAfter {

  System.setProperty(ClassicConstants.CONFIG_FILE_PROPERTY, "./src/main/scala/resources/logback.xml")

  val zbmedpp = new ZBMedppToXml
  val mongoTests = new MongoTests
  val qtdDocuments = 50
  val pathFile = "/home/javaapps/sbt-projects/ppzbmed_testunit.xml"

  val xDocuments: Seq[Document] = mongoTests.mongo.findAll.take(qtdDocuments)

  test(s"Tests the toXML method for $qtdDocuments documents to $pathFile) - toXml") {
    assert(zbmedpp.toXml(xDocuments, s"$pathFile").isSuccess)
  }
}
