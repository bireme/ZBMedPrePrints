#!/usr/bin/env bash

zbmedApi2mongodb() {
  $SBT_HOME/sbt "runMain zbmp2m.ZBMedPre2Mongo -database:ZBMEDTEST -collection:preprintsFullTest --reset -fromDate:2019-01-01 -host:172.17.1.230 --importByMonth" &> "logs/ZBMedPre2Mongo$NOW.log"
  ret="$?"
}

mongodbZbmed2xml() {
  $SBT_HOME/sbt "runMain Main -xmlout=/home/javaapps/testZBMed.xml -databaseRead=ZBMEDTEST -collectionRead=preprintsFullTest -hostRead=172.17.1.230"
}

cd /home/javaapps/sbt-projects/zbmed-mongodb || exit

NOW=$(date +"%Y%m%d%H%M%S")
SBT_HOME=/home/users/operacao/.local/share/coursier/bin

zbmedApi2mongodb

if [ "$ret" -ne 0 ]; then
  zbmedApi2mongodb

  if [ "$ret" -ne 0 ]; then
    sendemail -f appofi@bireme.org -u "ZBMedPre2Mongo ERROR - $(date '+%Y%m%d') (executado em : ${HOSTNAME})" -m "Error to import preprints from ZBMED into MongoDB" -t oliveirmic@paho.org,ofi@paho.org -s esmeralda.bireme.br
  else
    mongodbZbmed2xml
  fi

else
  mongodbZbmed2xml
fi

cd - || exit

exit $ret