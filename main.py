from etl.ingestion.DataIngestor import DataIngestor
from etl.transformation.DataValidatorAndTransformer import DataValidatorAndTransformer
import json 
from utils.logger_config import get_logger

logger = get_logger("Main")

def main():

    try:
        with open("config/metadata.json", "r") as f:
            metadata = json.load(f)
            
        dataflow = metadata["dataflows"][0]
        logger.info("✅ Metadatos cargados correctamente")

    except Exception as e:
        logger.error("❌ Error cargando los metadatos", exc_info=True)
        raise


    try:
        # Ingesta y escritura bronze
        logger.info("Procediendo a la ingesta de datos inicial en la tabla bronze...")

        ingestor = DataIngestor(spark) #clase que contiene los métodos relacionados con la ingesta de datos
        df_raw = ingestor.ingest(dataflow["sources"][0]) #se utiliza el método ingest para cargar el dataframe
        bronze_sink = next(s for s in dataflow["sinks"] if s["name"] == "empleados_data_bronze") #ubicación donde se realiza la ingesta inicial
        ingestor.save_as_table(df_raw, bronze_sink)

    except Exception as e:
        logger.error("❌ Error durante la ingesta", exc_info=True)
        raise


    try: 
        # Transformaciones y validaciones
        logger.info("Procediendo con las transformaciones y validaciones...")
        
        validator_and_transformer = DataValidatorAndTransformer(spark)
        validator_and_transformer.apply_sql_transformations(dataflow) #se crea la tabla silver y se separa de los registros que no han pasado la validación
        validator_and_transformer.add_validation_errors(dataflow) #se crea la tabla empledaos_data_ko con los registros que no han pasado la validación junto con los errores encontrados

    except Exception as e: 
        logger.error("❌ Error durante las transformaciones/validaciones", exc_info=True)
        raise
    
    #pasamos los metadatos como parámetro para la siguiente tarea
    dbutils.jobs.taskValues.set(key="metadata", value=json.dumps(metadata))

if __name__ == "__main__":
    main()
