package org.bireme.processing.tools.models

case class ZBMedpp_doc(id: String,
                       alternateId: String,
                       dbSource: String,
                       instance: String,
                       collection: String,
                       pType: String,
                       la: String,
                       fo: String,
                       dp: String,
                       pu: String,
                       ti: String,
                       aid: String,
                       ur: Array[String],
                       urPdf: Array[String],
                       fulltext: String,
                       ab: String,
                       au: Array[String],
                       entryDate: String,
                       da: String,
                       mj: Array[String])
                       //afiliacaoAutor: String,                 ***Dado indisponível***
                       //versionMedrxivBiorxiv: String,          ***Dado indisponível***
                       // license: String,                       ***Dado indisponível***
                       //typeDocumentMedrxivBiorxiv: String,     ***Dado indisponível***
                       //categoryMedrxivBiorxiv: String)         ***Dado indisponível***