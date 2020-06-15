import pyspark
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def add_increment(
        current_df: pyspark.sql.DataFrame,
        increment_df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    union_df = current_df.union(increment_df)
    return (
        union_df
        .withColumn(
            '_row_number',
            F.row_number().over(
                Window.partitionBy(union_df['link']).orderBy(['scraped_at'])
            )
        )
        .where(F.col('_row_number') == 1)
        .drop('_row_number')
    )
