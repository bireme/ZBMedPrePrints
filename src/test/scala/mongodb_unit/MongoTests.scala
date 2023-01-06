package mongodbl

import mongodb.MongoExport
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class MongoTests extends AnyFunSuite with BeforeAndAfter{

  var mongo: MongoExport = new MongoExport("ZBMed", "preprints", Option("localhost"), Option(27019))

  before{
    mongo
  }

  test("new pizza has zero toppings") {
    assert(mongo.findAll.nonEmpty)
  }
}
