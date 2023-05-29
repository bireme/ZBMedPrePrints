package mongodb_unit

import ch.qos.logback.classic.ClassicConstants
import services.MongoDB
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class MongoTests extends AnyFunSuite with BeforeAndAfter{

  System.setProperty(ClassicConstants.CONFIG_FILE_PROPERTY, "./src/main/scala/resources/logback.xml")

  val mongo: MongoDB = new MongoDB("ZBMed", "preprints_full", Option("172.17.1.71"), Option(27017))

  test("Validate session start with mongodb - checkLoginMongodb") {
    assert(mongo.checkLoginMongodb)
  }

  test(s"Check document collection - findAll [Total documents: ${mongo.findAll.length}]") {
    assert(mongo.findAll.nonEmpty)
  }
}
