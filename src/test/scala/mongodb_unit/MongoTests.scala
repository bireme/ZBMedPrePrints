package mongodb_unit

import ch.qos.logback.classic.ClassicConstants
import services.MongoDB
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class MongoTests extends AnyFunSuite with BeforeAndAfter{

  System.setProperty(ClassicConstants.CONFIG_FILE_PROPERTY, "./src/main/scala/resources/logback.xml")

  val mongo: MongoDB = new MongoDB("ZBMED_PPRINT", "preprints_full", Option("localhost"), Option(27017), append = true)

  test("Validate session start with mongodb - checkLoginMongodb") {
    assert(mongo.checkLoginMongodb)
  }

  test(s"Check document collection - findAll [Total documents: ${mongo.findAll.length}]") {
    assert(mongo.findAll.nonEmpty)
  }
}