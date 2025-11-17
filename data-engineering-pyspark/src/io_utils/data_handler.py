from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    FloatType, TimestampType, BooleanType, DoubleType
)

class DataHandler:
    """Realiza leitura/escrita de dados."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def _get_schema_pagamentos(self) -> StructType:
        return StructType([
            StructField("avaliacao_fraude", StructType([
                StructField("fraude", BooleanType(), True),
                StructField("score", DoubleType(), True),
            ])),
            StructField("data_processamento", StringType(), True),
            StructField("forma_pagamento", StringType(), True),
            StructField("id_pedido", StringType(), True),
            StructField("status", BooleanType(), True),
            StructField("valor_pagamento", DoubleType(), True)
        ])

    def _get_schema_pedidos(self) -> StructType:
        return StructType([
            StructField("ID_PEDIDO", StringType(), True),
            StructField("PRODUTO", StringType(), True),
            StructField("VALOR_UNITARIO", FloatType(), True),
            StructField("QUANTIDADE", LongType(), True),
            StructField("DATA_CRIACAO", TimestampType(), True),
            StructField("UF", StringType(), True),
            StructField("ID_CLIENTE", LongType(), True)
        ])

    def load_pagamentos(self, path: str) -> DataFrame:
        schema = self._get_schema_pagamentos()
        return (
            self.spark.read
                .schema(schema)
                .option("compression", "gzip")
                .json(path)
        )

    def load_pedidos(self, path: str, options: dict) -> DataFrame:
        schema = self._get_schema_pedidos()
        return (
            self.spark.read
                .options(**options)
                .schema(schema)
                .csv(path)
        )

    def write_parquet(self, df: DataFrame, path: str):
        df.write.mode("overwrite").parquet(path)
        print(f"✔ Arquivo gerado em: {path}")
