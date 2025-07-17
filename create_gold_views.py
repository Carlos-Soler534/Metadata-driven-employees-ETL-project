# Databricks notebook source
# MAGIC %md
# MAGIC En este notebook se pueden crear las vistas que se crean convenientes a partir de la tabla silver generada mediante el proceso ETL

# COMMAND ----------

import json

metadata = json.loads(dbutils.jobs.taskValues.get(taskKey="proceso_etl", key="metadata", debugValue="{}"))

dataflow = metadata["dataflows"][0] #conjunto de metadatos

silver_sink = next(s for s in dataflow["sinks"] if s["name"] == "empleados_data_silver") #localización de la tabla silver
catalog = silver_sink["catalog"]
schema = silver_sink["schema"]
name = silver_sink["name"]

silver_table = f"{catalog}.{schema}.{name}" #tabla silver 

# COMMAND ----------

gold_1 = "vw_salario_medio_por_oficina"
gold_1_location = f"{catalog}.{schema}.{gold_1}"
gold_2 = "vw_distr_categoriaSalario_por_oficina_y_departamento"
gold_2_location = f"{catalog}.{schema}.{gold_2}"

# COMMAND ----------

"""
se crea la primera vista gold: slaario medio por oficina
"""
spark.sql(f"""
          create or replace view {gold_1_location} as
          select office, avg(Salario) as salario_medio
          from {silver_table}
          group by office
          order by salario_medio desc
          """)

"""
se crea la segunda vista gold: distribución de categorias de salario por oficina y departamento
"""
spark.sql(f"""
          create or replace view {gold_2_location} as
          select office, Departamento, categoria_salario, count(*) as num_empleados
          from {silver_table}
          where Departamento != ''
          group by office, Departamento, categoria_salario
          order by office
          """)
