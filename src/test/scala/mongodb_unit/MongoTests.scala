package mongodb_unit

import ch.qos.logback.classic.ClassicConstants
import mongodb.MongoExport
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class MongoTests extends AnyFunSuite with BeforeAndAfter{

  System.setProperty(ClassicConstants.CONFIG_FILE_PROPERTY, "./src/main/scala/resources/logback.xml")

  val mongo: MongoExport = new MongoExport("ZBMed", "preprints", Option("localhost"), Option(27017))

  test("Validate session start with mongodb - checkLoginMongodb") {
    assert(mongo.checkLoginMongodb)
  }

  test(s"Check document collection - findAll [Total documents: ${mongo.findAll.length}]") {
    assert(mongo.findAll.nonEmpty)
  }
}