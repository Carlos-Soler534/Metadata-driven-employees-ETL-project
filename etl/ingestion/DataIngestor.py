from pyspark.sql.functions import col, array, lit, array_union, to_date, when
import json
from utils.logger_config import get_logger

logger = get_logger("DataIngestor")

class DataIngestor:
    def __init__(self, spark):
        self.spark = spark

    def ingest(self, source: dict):
        """
        lee y carga los datos como dataframe de spark.
            Args:
                spark: SparkSession
                source: metadatos correspondientes al origen y localización de los datos
            Returns:
                dataframe de spark con los datos cargados
        """
        logger.info(f"Iniciando ingesta desde {source['path']}")

        return self.spark.read.format(source["format"]).load(source["path"])

    def save_as_table(self, df, sink: dict):
        """
        guarda el dataframe como tabla siguiendo el catálogo,
        esquema y nombre de tabla indicados en los metadatos.
            Args:
                df: dataframe de spark
                sink: metadatos correspondientes al destino y localización donde se guardará la tabla
            Returns: 
                None (Simplemente se escribe y se guarda la tabla en la localización indicada)
        """
        table_name = sink["name"]
        logger.info(f"Guardando tabla: {table_name}")

        df.write.format(sink["format"]) \
            .mode(sink["saveMode"]) \
            .saveAsTable(f"{sink['catalog']}.{sink['schema']}.{sink['name']}")
            
        logger.info(f"✅ Guardada tabla: {sink['name']}")
