#!/usr/bin/env bash

SBT_HOME=/home/users/operacao/.local/share/coursier/bin

zbmedApi2mongodb() {
  $SBT_HOME/sbt "runMain org.bireme.processing.extractLoad.ZBMedPre2Mongo -database:ZBMED_PPRINT -collection:preprints_full --reset -fromDate:2019-01-01 -host:172.17.1.230 --importByMonth"
  ret="$?"
}

mongodbZbmed2xml_serverProd() {
  $SBT_HOME/sbt "runMain org.bireme.processing.transform.ZBMedMongo2XML -xmlout=/bases/iahx/xml-inbox/covidwho/ppzbmed_regional.xml -databaseRead=ZBMED_PPRINT -collectionRead=preprints_full -hostRead=172.17.1.230 -hostWrite=172.17.1.230 -collectionWrite=preprints_normalized"
}

mongodbZbmed2xml_miniMongo() {
  $SBT_HOME/sbt "runMain org.bireme.processing.transform.ZBMedMongo2XML -xmlout=/home/javaapps/sbt-projects/ZBMedPrePrints-processing/ppzbmed_regional.xml -databaseRead=ZBMED_PPRINT -collectionRead=preprints_full -hostRead=172.17.1.230  -collectionWrite=zbmed -hostWrite=200.10.179.230"
}


cd /home/javaapps/sbt-projects/ZBMedPrePrints-processing || exit

export SBT_OPTS="-Xms12g -Xmx18g -XX:+UseG1GC"

zbmedApi2mongodb

if [ "$ret" -eq 1 ]; then
  zbmedApi2mongodb

  if [ "$ret" -eq 1 ]; then
	sendemail -f appofi@bireme.org -u "ZBMedPrePrints-processing ERROR - $(date '+%Y%m%d') (executado em : ${HOSTNAME})" -m "Error to import preprints from ZBMED into MongoDB" -t olivmic@paho.org,ofi@paho.org -s esmeralda.bireme.br
  else
    mongodbZbmed2xml_serverProd
    mongodbZbmed2xml_miniMongo
  fi

else
  mongodbZbmed2xml_serverProd
  mongodbZbmed2xml_miniMongo
fi

cd - || exit

exit $ret
