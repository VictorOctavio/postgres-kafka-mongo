#!/bin/bash

# Función para crear conector
create_connector() {
    connector_name=$1
    json_config=$2

    echo "Creating connector: $connector_name"
    curl -X POST -H "Content-Type: application/json" --data "$json_config" http://localhost:8083/connectors
    echo
}

# Definir los conectores y sus configuraciones JSON
connectors=(
    "users-connector:{\"name\":\"users-connector\",\"config\":{\"connector.class\":\"io.debezium.connector.postgresql.PostgresConnector\",\"database.hostname\":\"postgres\",\"database.port\":\"5432\",\"database.user\":\"postgres\",\"database.password\":\"postgres\",\"database.dbname\":\"postgres\",\"database.server.name\":\"dbserver\",\"schema.include.list\":\"bank\",\"table.include.list\":\"bank.users\",\"topic.prefix\":\"dbserver\",\"plugin.name\":\"pgoutput\"}}"
    
    "infouser-connector:{\"name\":\"infouser-connector\",\"config\":{\"connector.class\":\"io.debezium.connector.postgresql.PostgresConnector\",\"database.hostname\":\"postgres\",\"database.port\":\"5432\",\"database.user\":\"postgres\",\"database.password\":\"postgres\",\"database.dbname\":\"postgres\",\"database.server.name\":\"dbserver\",\"schema.include.list\":\"bank\",\"table.include.list\":\"bank.infouser\",\"topic.prefix\":\"dbserver\",\"plugin.name\":\"pgoutput\",\"slot.name\":\"soltid_1\"}}"
    
    "accounts-connector:{\"name\":\"accounts-connector\",\"config\":{\"connector.class\":\"io.debezium.connector.postgresql.PostgresConnector\",\"database.hostname\":\"postgres\",\"database.port\":\"5432\",\"database.user\":\"postgres\",\"database.password\":\"postgres\",\"database.dbname\":\"postgres\",\"database.server.name\":\"dbserver\",\"schema.include.list\":\"bank\",\"table.include.list\":\"bank.accounts\",\"topic.prefix\":\"dbserver\",\"plugin.name\":\"pgoutput\",\"slot.name\":\"soltid_2\"}}"
)

# Iterar sobre los conectores y crearlos uno por uno
for connector in "${connectors[@]}"; do
    IFS=':' read -r connector_name json_config <<< "$connector"
    create_connector "$connector_name" "$json_config"
done
