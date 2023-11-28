#!/usr/bin/env bash

# Definição da variável para o diretório do SBT
SBT_HOME=/home/users/operacao/.local/share/coursier/bin

# Método que obtém documentos da API ZBMED e escreve no MongoDB.
zbmedApi2mongodb() {

  DATABASE="-database:ZBMED_PPRINT"                                         #- (-database:<name>)                   MongoDB database collection name
  COLLECTION="-collection:preprints_full"                                   #- (-collection:<name>)                 MongoDB database collection name
  DECS_DATABASE="-decsDatabase:DECS"                                        #- (-decsDatabase:<name>)               MongoDB DeCS database name. Default is 'DECS'
  FROM_DATE="-fromDate:2019-01-01"                                          #- (-fromDate:<yyyy-mm-dd>)             initial date or number of days before today
  HOST="-host:172.17.1.230"                                                 #- (-host:<name>)                       MongoDB server name. Default value is 'localhost'
  RESET="--reset"                                                           #- (--reset)                            initializes the MongoDB collection if it is not empty
  IMPORT_BY_MONTH="--importByMonth"                                         #- (--importByMonth)                    if present, the system will load documents month by month
  DECS_COLLECTION=""                                                        #- (-decsCollection:<name>)             MongoDB DeCS database collection name. Default is the current year
  TO_DATE=""                                                                #- (-toDate:<yyyy-mm-dd>)               end date. Default is today
  QUANTITY=""                                                               #- (-quantity:<num>)                    number of preprints to import. Default is unlimited
  EXCLUDE_SOURCES=""                                                        #- (-excludeSources:<src1>,...,<srcn>)  exclude documentos whose sources is here listed
  PORT=""                                                                   #- (-port:<number>)                     MongoDB server port number. Default value is 27017
  USER=""                                                                   #- (-user:<name>)                       MongoDB user name. Default is not to use an user
  PASSWORD=""                                                               #- (-password:<pwd>)                    MongoDB user password. Default is not to use an password
  CHECK_REPEATABLE=""                                                       #- (--checkRepeatable)                  if present, the system will check if the document was not already inserted (id check)
  INDEXNAME="-indexName:id"                                                 #- (-indexName:<name>)                  parameter to determine the name of the field that will take on the role of collection index

  $SBT_HOME/sbt "runMain org.bireme.processing.extractLoad.ZbmedToMongoDb $CLASS_MAIN_ZBMED_API $DATABASE $COLLECTION $RESET $FROM_DATE $HOST $IMPORT_BY_MONTH $DECS_DATABASE $INDEXNAME"
  ret="$?"
}

# Método que lê os documentos escritos no MongoDB, transforma, reescreve no MongoDB em nova coleção e por fim gera um XML.
mongodbZbmed2xml_serverProd() { # Executando para servidor: 172.17.1.230

  XML_OUT="-xmlout=/bases/iahx/xml-inbox/covidwho/ppzbmed_regional.xml"     #- (-xmlout=)           XML file output directory
  DATABASE_READ="-databaseRead=ZBMED_PPRINT"                                #- (-databaseRead=)     MongoDB database name
  COLLECTION_READ="-collectionRead=preprints_full"                          #- (-collectionRead=)   MongoDB database collection name
  HOST_READ="-hostRead=172.17.1.230"                                        #- (-hostRead=)         MongoDB server name. Default value is 'localhost'
  COLLECTION_WRITE="-collectionWrite=preprints_normalized"                  #- (-collectionWrite=)  MongoDB database collection name Normalized
  HOST_WRITE="-hostWrite=172.17.1.230"                                      #- (-hostWrite=)        MongoDB server name. Default value is 'localhost' Normalized
  PORT_READ=""                                                              #- (-portRead=)         MongoDB server port number. Default value is 27017
  USER_READ=""                                                              #- (-userRead=)         MongoDB user name
  PASSWORD_READ=""                                                          #- (-passwordRead=)     MongoDB user password
  DATABASE_WRITE=""                                                         #- (-databaseWrite=)    MongoDB database name Normalized
  PORT_WRITE=""                                                             #- (-portWrite=)        MongoDB server port number. Default value is 27017 Normalized
  USER_WRITE=""                                                             #- (-userWrite=)        MongoDB user name Normalized
  PASSWORD_WRITE=""                                                         #- (-passwordWrite=)    MongoDB user password Normalized
  APPEND=""                                                                 #- (--append)           If present, will compose the collection without clearing it first
  INDEXNAME="-indexName=id"                                                 #- (-indexName=<name>)  parameter to determine the name of the field that will take on the role of collection index

  $SBT_HOME/sbt "runMain org.bireme.processing.transform.ZbmedMongoDbToXml $CLASS_MAIN_ZBMED_XML $XML_OUT $DATABASE_READ $COLLECTION_READ $HOST_READ $HOST_WRITE $COLLECTION_WRITE $INDEXNAME"
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
  INDEXNAME_XML="-indexName=id"

  $SBT_HOME/sbt "runMain org.bireme.processing.transform.ZbmedMongoDbToXml $CLASS_MAIN_ZBMED_XML $XML_OUT_BACKUP $DATABASE_READ_XML $COLLECTION_READ_XML $HOST_READ_XML $COLLECTION_WRITE_XML $HOST_WRITE_XML $INDEXNAME_XML"
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
