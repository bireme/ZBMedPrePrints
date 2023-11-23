package zbmed_tests

import org.bireme.processing.tools.mrw.{MongoDbReader, MongoDbWriter, mdrParameters, mdwParameters}
import org.scalatest.funsuite.AnyFunSuite

class MongoDbTests extends AnyFunSuite {

  test("Validates if the return is successful for the query in: base: ZBMED_PPRINT, collection: preprints_full, and host: 172.17.1.230") {

    val parameters: mdrParameters = mdrParameters(database = "ZBMED_PPRINT", collection = "preprints_full",
      host = Option("172.17.1.230"), quantity = Option(20))
    val mongoDbReader: MongoDbReader = new MongoDbReader(parameters)

    assert(mongoDbReader.iterator().isSuccess)
  }

  test("Validates if the return is successful for document insertion into: base: UNITARY_TESTS, collection: zbmed_pprint, and host: 172.17.1.230") {

    val parameters: mdwParameters = mdwParameters(database = "UNITARY_TESTS", collection = "zbmed_pprint", clear = false,
      host = Option("172.17.1.230"), indexName = Option("Unitary"))
    val mongoDbReader: MongoDbWriter = new MongoDbWriter(parameters)

    assert(mongoDbReader.insertDocument(Map("Unitary Test" -> "ZBMED_PPRINT")).isSuccess)
  }
}