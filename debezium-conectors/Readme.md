# POST -> http://localhost:8083/connectors
# chmod +x create_connectors.sh
{
    "name": "users-connector",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "database.hostname": "postgres",
       "database.port": "5432",
       "database.user": "postgres",
       "database.password": "postgres",
       "database.dbname" : "postgres",
       "database.server.name": "dbserver1",
       "schema.include.list": "bank",
       "table.include.list": "bank.users",
       "topic.prefix": "dbserver"
    }
}      

{
    "name": "infouser-connector",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "database.hostname": "postgres",
       "database.port": "5432",
       "database.user": "postgres",
       "database.password": "postgres",
       "database.dbname" : "postgres",
       "database.server.name": "dbserver1",
       "schema.include.list": "bank",
       "table.include.list": "bank.infouser",
       "topic.prefix": "dbserver",
       "slot.name": "soltid_1"
    }
}      

{
    "name": "accounts-connector",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "database.hostname": "postgres",
       "database.port": "5432",
       "database.user": "postgres",
       "database.password": "postgres",
       "database.dbname" : "postgres",
       "database.server.name": "dbserver1",
       "schema.include.list": "bank",
       "table.include.list": "bank.accounts",
       "topic.prefix": "dbserver",
       "slot.name": "soltid_2"
    }
}      