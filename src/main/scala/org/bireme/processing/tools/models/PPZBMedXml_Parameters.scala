package org.bireme.processing.tools.models

case class PPZBMedXml_Parameters(xmlOut: String,
                                 databaseRead: String,
                                 collectionRead: String,
                                 hostRead: Option[String],
                                 portRead: Option[Int],
                                 userRead: Option[String],
                                 passwordRead: Option[String],
                                 databaseWrite: Option[String],
                                 collectionWrite: Option[String],
                                 hostWrite: Option[String],
                                 portWrite: Option[Int],
                                 userWrite: Option[String],
                                 passwordWrite: Option[String],
                                 append: Boolean,
                                 indexName: Option[String])
