import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql.types import DataType, DateType, DecimalType, StructType


def format_date(input_df: DataFrame, original_name: str, field_name: str) -> DataFrame:
    """
    Format date columns to spark date format

    :param input_df: Spark DataFrame to be formatted
    :param original_name: column name of column to be formatted
    :param field_name: new column name
    :return: Spark DataFrame formatted column
    """
    output_df = input_df.withColumn(
        field_name,
        f.when(
            f.to_date(f.col(original_name), "dd/MM/yyyy").isNotNull(),
            f.to_date(f.to_date(f.col(original_name), "dd/MM/yyyy")),
        )
        .when(
            f.to_date(f.col(original_name), "d-M-yyyy").isNotNull(),
            f.to_date(f.to_date(f.col(original_name), "d-M-yyyy")),
        )
        .when(
            f.to_date(f.col(original_name), "yyyyMMdd").isNotNull(),
            f.to_date(f.to_date(f.col(original_name), "yyyyMMdd")),
        )
        .when(
            f.to_date(f.col(original_name), "yyyy-MM-dd").isNotNull(),
            f.to_date(f.to_date(f.col(original_name), "yyyy-MM-dd")),
        )
        .when(
            f.to_date(f.col(original_name), "ddMMyyyy").isNotNull(),
            f.to_date(f.to_date(f.col(original_name), "ddMMyyyy")),
        )
        .when(
            f.to_date(f.col(original_name), "dd.MM.yyyy").isNotNull(),
            f.to_date(f.to_date(f.col(original_name), "dd.MM.yyyy")),
        )
        .when(
            f.to_date(f.col(original_name), "dd-MM-yyyy").isNotNull(),
            f.to_date(f.to_date(f.col(original_name), "dd-MM-yyyy")),
        ),
    )
    return output_df


def format_decimal(
    input_df: DataFrame, original_name: str, field_name: str, date_type: DataType
) -> DataFrame:
    """Strip leading and trailing brackets using regexp_replace"""
    output_df = input_df.withColumn(
        field_name,
        f.when(
            f.col(original_name).rlike(r"^\(.*\)$"),
            -1
            * f.regexp_replace(input_df[original_name], r"[\(\),]", "").cast(date_type),
        ).otherwise(f.expr(f"replace({original_name}, ',', '')").cast(date_type)),
    )
    return output_df


def apply_schema(input_df: DataFrame, schema: StructType) -> DataFrame:
    """
    Apply a schema to a Spark Dataframe. Note that an imcompatible cast results in
    an empty value for that column. This is the responsibility of the user to make
    good data contracts with their suppliers. (We could alert for this)

    :param input_df: Spark DataFrame to be formatted
    :param schema: Schema that will be applied to the DataFrame
    :return: Spark DataFrame with schema applied.
    """
    for field in schema.fields:
        original_name = (
            field.metadata["original_name"]
            if "original_name" in field.metadata
            else field.name
        )
        if isinstance(field.dataType, DateType):
            input_df = format_date(input_df, original_name, field.name)
        elif isinstance(field.dataType, DecimalType):
            input_df = format_decimal(
                input_df, original_name, field.name, field.dataType
            )
        else:
            input_df = input_df.withColumn(
                field.name, f.col(original_name).cast(field.dataType)
            )
    output_df = input_df.to(schema)
    return output_df
