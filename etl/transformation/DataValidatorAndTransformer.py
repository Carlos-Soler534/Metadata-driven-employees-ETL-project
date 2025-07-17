from pyspark.sql.functions import col, array, lit, array_union, to_date, when
import json
from utils.logger_config import get_logger

logger = get_logger("DataValidatorAndTransformer")

class DataValidatorAndTransformer:
    def __init__(self, spark):
        self.spark = spark

    def apply_sql_transformations(self, dataflow: dict):
        """
        Aplica las transformaciones SQL indicadas en los metadatos sobre la tabla bronze,
        guardando cada resultado como tabla en catálogo y esquema indicados.
            Args:
                spark: SparkSession
                dataflow: conjunto de metadatos
            Returns:
                None (Simplemente se ejecutan las sentencias SQL sobre la tabla bronze y se crean las tablas transformadas correspondientes)
        """

        # Asumimos que la tabla bronze ya existe y será la tabla base para las primeras transformaciones.
        # Por ejemplo, las sentencias SQL de las transformaciones deberían partir de "bronze_table_name"
        # o de tablas intermedias que esta función va guardando.
        
        for step in dataflow["transformations"]:
            query = step["params"]["sql"]
            table_name = step["name"]
            sink = next(s for s in dataflow["sinks"] if s["name"] == table_name)

            logger.info(f"Aplicando transformación: {table_name}")

            df_step = self.spark.sql(query)

            full_table_name = f"{sink['catalog']}.{sink['schema']}.{sink['name']}"
            df_step.write.format(sink["format"]).mode(sink["saveMode"]).saveAsTable(full_table_name)

            logger.info(f"✅ Guardada tabla: {full_table_name}")

            if table_name == "empleados_data_silver":
                validacion_ok_table = f"{sink['catalog']}.{sink['schema']}.validacion_ok"
                self.spark.sql(f"DROP TABLE IF EXISTS {validacion_ok_table}")

                logger.info(f"🧹 Borrada tabla intermedia: {validacion_ok_table}")


    def get_validation_rules(self):
        """
        se definen las reglas de validación que se aplican sobre los campos.
            Args:
                None
            Returns:
                diccionario con las reglas de validación que se aplican sobre los campos
        """
        return {
            "notNull": lambda c: col(c).isNotNull(),
            "notEmpty": lambda c: col(c) != "",
            "nonNegative": lambda c: col(c) >= 0,
            "isDate": lambda c: to_date(col(c), "yyyy-MM-dd").isNotNull(),
            "validOffice": lambda c: col(c).isin("ESP", "UK", "FR", "NT", "BEL", "ITA", "GER")
        }


    def add_validation_errors(self, dataflow: dict):
        """
        se añaden el campo que muestra los errores de validación
        encontrados en el dataframe KO.
        Args:
            spark: SparkSession
            ko_sink: metadatos correspondientes al destino y localización donde se guardará la tabla con el campo de errores generado
            ko_val_sink: metadatos correspondientes al destino y localización donde se encuentra la tabla con los registros que no han 
                        superado el proceso de validación
            field_validations: metadatos correspondientes a las reglas de validación que se aplican sobre los campos
            validation_rules: reglas de validación que se aplican sobre los campos
        Returns:
            None (Simplemente se añade el campo de errores generado sobre el dataframe KO y se guarda como tabla en la localización indicada)
        """
        
        ko_sink = next(s for s in dataflow["sinks"] if s["name"] == "empleados_data_ko")
        ko_val_sink = next(s for s in dataflow["sinks"] if s["name"] == "validacion_ko")
        field_validations = dataflow["field_validations"]
        rules = self.get_validation_rules()

        df_ko_val = self.spark.table(f"{ko_val_sink['catalog']}.{ko_val_sink['schema']}.{ko_val_sink['name']}") \
            .withColumn("data_quality_violation", array())

        logger.info("Añadiendo errores de validación...")

        for rule in field_validations:
            field = rule["field"]
            for val in rule["rules"]:
                cond = rules[val](field)
                tag = f"{field}_{val}"

                df_ko_val = df_ko_val.withColumn(
                    "data_quality_violation",
                    when(~cond, array_union(col("data_quality_violation"), array(lit(tag))))
                    .otherwise(col("data_quality_violation"))
                )

        df_ko_val.write.format(ko_sink["format"]) \
            .mode(ko_sink["saveMode"]).saveAsTable(f"{ko_sink['catalog']}.{ko_sink['schema']}.{ko_sink['name']}")

        self.spark.sql(f"DROP TABLE IF EXISTS {ko_val_sink['catalog']}.{ko_val_sink['schema']}.{ko_val_sink['name']}")
        logger.info(f"🧹 Borrada tabla intermedia: {ko_val_sink['name']}")
