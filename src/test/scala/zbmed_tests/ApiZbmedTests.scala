package zbmed_tests

import org.bireme.processing.extractLoad.ZbmedToMongoDb
import org.scalatest.funsuite.AnyFunSuite

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class ApiZbmedTests extends AnyFunSuite {

  test("ZBMed API should return a status code of 200") {

    val dataFromString: String = "2023-11-01"
    val dataToString: String = "2023-11-23"
    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val fromDate: LocalDate = LocalDate.parse(dataFromString, formatter)
    val toDate: LocalDate = LocalDate.parse(dataToString, formatter)
    val status_code: Int = ZbmedToMongoDb.callApi(0, 50, fromDate, toDate, formatter).get.statusCode()

    assert(status_code == 200)
  }
}