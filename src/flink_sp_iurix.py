import os
import logging
import json
from datetime import datetime

from pyflink.datastream import *
from pyflink.table import *
from pyflink.table.catalog import JdbcCatalog

# Variables de Entorno
RUNTIME_ENV = os.getenv("RUNTIME_ENV", "local")



if __name__ == "__main__":
    """
    ## cluster execution
    docker exec jobmanager /opt/flink/bin/flink run \
        --python /tmp/src/flink_sp_iurix.py \
        -d
        
    ## Deter y guardar SavePoint
    docker exec jobmanager /opt/flink/bin/flink stop \
        --type canonical [IdJOB] \
        -d
        
    ## Iniciar Job desde SavePoint
    docker exec jobmanager /opt/flink/bin/flink run \
        -s file:///tmp/src/savepoints/[FolderSavePoint] \
        --python /tmp/src/flink_sp_iurix.py \
        -d
        
    ## Iniciar Job desde CheckPoint
    docker exec jobmanager /opt/flink/bin/flink run \
        -s file:///tmp/src/checkpoint/[FolderChakePoint]/[chk-id] \
        --python /tmp/src/flink_sp_iurix.py \
        -d
    """

    # ----------CONFIGURACIONES FLINK----------

    # Configuración del registro de eventos
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d:%(levelname)s:%(name)s:%(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logging.info(f"RUNTIME_ENV - {RUNTIME_ENV}")

    # Obtener el entorno de ejecución de PyFlink para Datastream
    env = StreamExecutionEnvironment.get_execution_environment()

    # Obtener entorno de ejecución de Pyflink para Table API 
    table_env = StreamTableEnvironment.create(env)

    # ---------- CONFIGURAR CONEXION A LA BASE ----------
    name = "my_catalogo"
    default_database = "postgres"
    username = "postgres"
    password = "postgres"
    base_url = "jdbc:postgresql://localhost:5432"

    catalog = JdbcCatalog(name, default_database, username, password, base_url)
    table_env.register_catalog("my_catalogo", catalog)
    table_env.use_catalog("my_catalog")

    listaCatalogos = table_env.list_catalogs()
    print("Catálogos disponibles:", listaCatalogos)

    # ---------- EJECUCION DE LA TAREA ----------

    env.execute("sp_iurix")