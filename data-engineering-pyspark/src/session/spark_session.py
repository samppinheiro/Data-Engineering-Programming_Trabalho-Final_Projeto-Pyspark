from pyspark.sql import SparkSession

class SparkSessionManager:
    """Gerencia a criação da SparkSession."""

    @staticmethod
    def get_spark_session(app_name: str) -> SparkSession:
        return (
            SparkSession.builder
                .appName(app_name)
                .master("local[*]")
                .config("spark.sql.shuffle.partitions", "4")
                .config("spark.ui.showConsoleProgress", "true")
                .getOrCreate()
        )
