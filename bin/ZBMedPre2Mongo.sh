#!/usr/bin/env bash

# Definição da variável para o diretório do SBT
SBT_HOME=/home/users/operacao/.local/share/coursier/bin

# Método que obtém documentos da API ZBMED e escreve no MongoDB.
zbmedApi2mongodb() {

  DATABASE="-database:ZBMED_PPRINT"                                          #- MongoDB database collection name
  COLLECTION="-collection:preprints_full"                                    #- MongoDB database collection name
  DECS_DATABASE="DECS"                                                       #- MongoDB DeCS database name. Default is 'DECS'
  FROM_DATE="-fromDate:2019-01-01"                                           #- daysBefore:<days>)] - initial date or number of days before today
  HOST="-host:172.17.1.230"                                                  #- MongoDB server name. Default value is 'localhost'
  RESET="--reset"                                                            #- initializes the MongoDB collection if it is not empty
  IMPORT_BY_MONTH="--importByMonth"                                          #- if present, the system will load documents month by month
  DECS_COLLECTION=""                                                         #- MongoDB DeCS database collection name. Default is the current year
  TO_DATE=""                                                                 #- end date. Default is today
  QUANTITY=""                                                                #- number of preprints to import. Default is unlimited
  EXCLUDE_SOURCES=""                                                         #- exclude documentos whose sources is here listed
  PORT=""                                                                    #- MongoDB server port number. Default value is 27017
  USER=""                                                                    #- MongoDB user name. Default is not to use an user
  PASSWORD=""                                                                #- MongoDB user password. Default is not to use an password
  CHECK_REPEATABLE="--checkRepeatable"                                       #- if present, the system will check if the document was not already inserted (id check)

  $SBT_HOME/sbt "runMain org.bireme.processing.extractLoad.ZBMedPre2Mongo $CLASS_MAIN_ZBMED_API $DATABASE $COLLECTION $RESET $FROM_DATE $HOST $IMPORT_BY_MONTH $DECS_DATABASE"
  ret="$?"
}

# Método que lê os documentos escritos no MongoDB, transforma, reescreve no MongoDB em nova coleção e gera um XML.
mongodbZbmed2xml_serverProd() { # Executando para servidor: 172.17.1.230

  XML_OUT="-xmlout=/bases/iahx/xml-inbox/covidwho/ppzbmed_regional.xml"     #- XML file output directory
  DATABASE_READ="-databaseRead=ZBMED_PPRINT"                                #- MongoDB database name
  COLLECTION_READ="-collectionRead=preprints_full"                          #- MongoDB database collection name
  HOST_READ="-hostRead=172.17.1.230"                                        #- MongoDB server name. Default value is 'localhost'
  COLLECTION_WRITE="-collectionWrite=preprints_normalized"                  #- MongoDB database collection name Normalized
  HOST_WRITE="-hostWrite=172.17.1.230"                                      #- MongoDB server name. Default value is 'localhost' Normalized
  PORT_READ=""                                                              #- MongoDB server port number. Default value is 27017
  USER_READ=""                                                              #- MongoDB user name
  PASSWORD_READ=""                                                          #- MongoDB user password
  DATABASE_WRITE=""                                                         #- MongoDB database name Normalized
  PORT_WRITE=""                                                             #- MongoDB server port number. Default value is 27017 Normalized
  USER_WRITE=""                                                             #- MongoDB user name Normalized
  PASSWORD_WRITE=""                                                         #- MongoDB user password Normalized
  APPEND=""                                                                 #- If present, will compose the collection without clearing it first

  $SBT_HOME/sbt "runMain org.bireme.processing.transform.ZBMedMongo2XML $CLASS_MAIN_ZBMED_XML $XML_OUT $DATABASE_READ $COLLECTION_READ $HOST_READ $HOST_WRITE $COLLECTION_WRITE"
}

mongodbZbmed2xml_miniMongo() { # Executando novamente para servidor: 200.10.179.230

    XML_OUT_BACKUP="-xmlout=/home/javaapps/sbt-projects/ZBMedPrePrints-processing/ppzbmed_regional.xml"
    DATABASE_READ_XML="-databaseRead=ZBMED_PPRINT"
    COLLECTION_READ_XML="-collectionRead=preprints_full"
    HOST_READ_XML="-hostRead=172.17.1.230"
    COLLECTION_WRITE_XML="-collectionWrite=zbmed"
    HOST_WRITE_XML="-hostWrite=200.10.179.230"
    PORT_READ_XML=""
    USER_READ_XML=""
    PASSWORD_READ_XML=""
    DATABASE_WRITE_XML=""
    PORT_WRITE_XML=""
    USER_WRITE_XML=""
    PASSWORD_WRITE_XML=""
    APPEND_XML=""

  $SBT_HOME/sbt "runMain org.bireme.processing.transform.ZBMedMongo2XML $CLASS_MAIN_ZBMED_XML $XML_OUT_BACKUP $DATABASE_READ_XML $COLLECTION_READ_XML $HOST_READ_XML $COLLECTION_WRITE_XML $HOST_WRITE_XML"
}

# Diretório local do projeto
cd /home/javaapps/sbt-projects/ZBMedPrePrints-processing || exit

# Definição da utilização de memória na execução
export SBT_OPTS="-Xms12g -Xmx18g -XX:+UseG1GC"

zbmedApi2mongodb

if [ "$ret" -eq 1 ]; then # Se o retorno da execução de zbmedApi2mongodb for 1 (erro), então processará novamente
  zbmedApi2mongodb

  if [ "$ret" -eq 1 ]; then # Se o retorno da execução de zbmedApi2mongodb for 1 (erro) novamente, então enviará um e-mail informando ERRO e finalizará
    sendemail -f appofi@bireme.org -u "ZBMedPrePrints-processing ERROR - $(date '+%Y%m%d') (executado em: ${HOSTNAME})" -m "Error importing preprints from ZBMED into MongoDB" -t olivmic@paho.org,ofi@paho.org -s esmeralda.bireme.br
  else # Se no segundo processamento a quantidade do processamento atual for maior que a quantidade de documentos contida no MongoDB, então executará os próximos métodos de transformação
    mongodbZbmed2xml_serverProd
    mongodbZbmed2xml_miniMongo
  fi

else # Se o processamento do primeiro método tem a quantidade de documentos maior que a quantidade de documentos contida no MongoDB, então executará os próximos métodos de transformação
  mongodbZbmed2xml_serverProd
  mongodbZbmed2xml_miniMongo
fi

cd - || exit

exit $ret
