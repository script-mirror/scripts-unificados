#!/bin/bash

# Carregar variáveis de ambiente do arquivo .env
# export $(grep -v '^#' .env | xargs)

# Verifique se a data foi fornecida como argumento
if [ -z "$1" ]; then
    echo "Uso: $0 <data_no_formato_YYYYMMDD>"
    exit 1
fi

# Data fornecida via linha de comando
DATA=$1
CONTAINER_NAME="db_mysql_new"
MYSQL_USER="climenergy"
MYSQL_PASSWORD="climeserver"


# Declare um array associativo com o nome do banco de dados como chave e o padrão de arquivo de dump como valor
declare -A DATABASES
DATABASES=(
#    ["bbce"]="dump-bbce-${DATA}.sql"
#    ["carteira"]="dump-carteira-${DATA}.sql"
#    ["db_book_gd"]="dump-db_book_gd-${DATA}.sql"
    ["db_carga_consumidor"]="dump-db_carga_consumidor-${DATA}.sql"
#    ["db_config"]="dump-db_config-${DATA}.sql"
#    ["db_decks"]="dump-db_decks-${DATA}.sql"
#    ["db_meteorologia"]="dump-db_meteorologia-${DATA}.sql"
#    ["db_ons"]="dump-db_ons-${DATA}.sql"
#    ["db_ons_dados_abertos"]="dump-db_ons_dados_abertos-${DATA}.sql"
#    ["db_pluvia"]="dump-db_pluvia-${DATA}.sql"
#    ["db_rodadas"]="dump-db_rodadas-${DATA}.sql"
#    ["db_tmp"]="dump-db_tmp-${DATA}.sql"
    # ["climenergy"]="dump-climenergy-${DATA}.sql"
)

# Iterar sobre o array e verificar se o arquivo de dump existe
for DB_NAME in "${!DATABASES[@]}"; do
    DUMP_FILE=${DATABASES[$DB_NAME]}
    
    DUMP_PATH="/WX/WX2TB/Documentos/fontes/PMO/scripts_unificados/server_configuration/containers/airflow_mysql/dumps/$DUMP_FILE"
    
    if [ -f "$DUMP_PATH" ]; then
        echo "Importando $DUMP_FILE para o banco de dados $DB_NAME"
        docker exec -i $CONTAINER_NAME mysql -u $MYSQL_USER -p"$MYSQL_PASSWORD" $DB_NAME < $DUMP_PATH
    else
        echo "Arquivo de dump $DUMP_FILE não encontrado para o banco de dados $DB_NAME. Pulando..."
    fi
done

echo "Importação concluída."

