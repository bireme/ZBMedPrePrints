package zbmed_tests

import org.bireme.processing.export.ZbmedToXml
import org.bson.Document
import org.scalatest.funsuite.AnyFunSuite

import scala.jdk.CollectionConverters._

class ZbmedTests extends AnyFunSuite {

  val document: Document = new Document(
    Map(
      "_id" -> Map("$oid" -> "655e2eb3a428837ebc8d8853"),
      "abstract" -> "The massive availability of cameras results in a wide variability of imaging conditions.",
      "all_annotations" -> Array(
        Map(
          "class" -> "MESHD",
          "concept" -> "MESHD:http://purl.bioontology.org/ontology/MESH/D000086382",
          "descriptor_en" -> "COVID-19",
          "descriptor_es" -> "COVID-19",
          "descriptor_fr" -> "COVID-19",
          "descriptor_pt" -> "COVID-19",
          "id" -> "D000086382",
          "mfn" -> "59585"
        )
      ),
      "date" -> "2019-02-21T00:00:00",
      "docLink" -> "http://arxiv.org/abs/1902.08123v4",
      "id" -> "1902.08123v4",
      "link" -> List("http://arxiv.org/abs/1902.08123v4", "http://arxiv.org/pdf/1902.08123v4").asJava,
      "pdfLink" -> "http://arxiv.org/pdf/1902.08123v4",
      "rel_authors" -> Array(
        Map("author_inst" -> "", "author_name" -> "Fernando Alonso-Fernandez"),
        Map("author_inst" -> "", "author_name" -> "Kiran B. Raja"),
        Map("author_inst" -> "", "author_name" -> "R. Raghavendra"),
        Map("author_inst" -> "", "author_name" -> "Cristoph Busch"),
        Map("author_inst" -> "", "author_name" -> "Josef Bigun"),
        Map("author_inst" -> "", "author_name" -> "Ruben Vera-Rodriguez"),
        Map("author_inst" -> "", "author_name" -> "Julian Fierrez")
      ).map(_.asJava),
      "source" -> "arxiv",
      "title" -> "Cross-Sensor Periocular Biometrics for Partial Face Recognition in a Global Pandemic: Comparative Benchmark and Novel Multialgorithmic Approach"
    ).asJava
  )

  val zbmedpp: ZbmedToXml = new ZbmedToXml
  val pathFile: String = "/home/javaapps/sbt-projects/ZBMedPrePrints-processing/ppzbmed_unitarytest.xml"

  test(s"Validates if the return is successful for the XML generation at: $pathFile.") {
    assert(zbmedpp.normalizeData(Seq(document), pathFile).isSuccess)
  }
}