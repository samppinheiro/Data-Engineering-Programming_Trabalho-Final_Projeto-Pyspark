from pyspark.sql import DataFrame
from pyspark.sql import functions as F

class Transformation:

    def filtrar_pagamentos_validos(self, pagamentos_df: DataFrame) -> DataFrame:
        return (
            pagamentos_df
                .filter(F.col("status") == False)
                .filter(F.col("fraude") == False)
        )

    def filtrar_pedidos_2025(self, pedidos_df: DataFrame) -> DataFrame:
        return pedidos_df.filter(F.year("DATA_CRIACAO") == 2025)

    def adicionar_valor_total(self, pedidos_df: DataFrame) -> DataFrame:
        return pedidos_df.withColumn(
            "valor_total",
            F.col("VALOR_UNITARIO") * F.col("QUANTIDADE")
        )

    def join_pedidos_pagamentos(self, pedidos_df, pagamentos_df):
        return pedidos_df.join(
            pagamentos_df,
            pedidos_df.ID_PEDIDO == pagamentos_df.id_pedido,
            "inner"
        )

    def selecionar_campos_finais(self, df):
        return df.select(
            df.ID_PEDIDO.alias("id_pedido"),
            df.UF.alias("estado"),
            df.forma_pagamento,
            df.valor_total,
            df.DATA_CRIACAO.alias("data_pedido")
        )

    def ordenar_relatorio(self, df):
        return df.orderBy("estado", "forma_pagamento", "data_pedido")

