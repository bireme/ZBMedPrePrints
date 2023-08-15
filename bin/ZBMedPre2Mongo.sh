#!/usr/bin/env bash

zbmedApi2mongodb() {
  $SBT_HOME/sbt "runMain ZBMedPre2Mongo -database:ZBMEDPPRINT -collection:preprintsfull --reset -fromDate:2019-01-01 -host:172.17.1.230 --importByMonth"
  ret="$?"
}

mongodbZbmed2xml() {
  $SBT_HOME/sbt "runMain ZBMedMongo2XML -xmlout=/bases/iahx/xml-inbox/covidwho/ppzbmed_regional.xml -databaseRead=ZBMEDPPRINT -collectionRead=preprintsfull -hostRead=172.17.1.230"
}

cd /home/javaapps/sbt-projects/ZBMedPrePrints || exit

SBT_HOME=/home/users/operacao/.local/share/coursier/bin

export SBT_OPTS="-Xms12g -Xmx18g -XX:+UseG1GC"

zbmedApi2mongodb

if [ "$?" -ne 0 ]; then
  zbmedApi2mongodb

  if [ "$?" -ne 0 ]; then
    sendemail -f appofi@bireme.org -u "ZBMedPre2Mongo ERROR - $(date '+%Y%m%d') (executado em : ${HOSTNAME})" -m "Error to import preprints from ZBMED into MongoDB" -t oliveirmic@paho.org,ofi@paho.org -s esmeralda.bireme.br
  else
    mongodbZbmed2xml
  fi

else
  mongodbZbmed2xml
fi

cd - || exit

exit $ret