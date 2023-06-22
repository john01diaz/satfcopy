import os
import shutil
from deltalake import DeltaTable
import pyspark
from delta import configure_spark_with_delta_pip
from nf_common_source.code.services.file_system_service.objects.folders import Folders


def load_and_register_table(
        file_configuration: list,
        output_root_folder: Folders) \
        -> None:
    match file_configuration[1]:
        case 'parquet':
            try:
                pyarrow_table = \
                    __get_parquet_table(
                        file_configuration=file_configuration)

                __export_pyarrow_table_to_parquet(
                    output_root_folder=output_root_folder,
                    pyarrow_table=pyarrow_table,
                    stage_root_folder_name=file_configuration[0].split(os.sep)[-1],
                    stage=file_configuration[2],
                    parquet_folder_name=file_configuration[3])

            except Exception as error:
                print('*'*30 + ' ERROR: ' + file_configuration[3] + ' - ' + str(error))

        case other:
            raise \
                NotImplementedError


def __get_parquet_table(
        file_configuration: list):
    user_specific_absolute_path_to_relative_path = \
        file_configuration[0]

    relative_path = \
        file_configuration[2]

    file_name_folder = \
        file_configuration[3]

    print(
        'Running on: ' + str(relative_path) + ' - ' + str(file_name_folder))

    absolute_file_name_folder_path = \
        os.path.join(
            user_specific_absolute_path_to_relative_path,
            relative_path,
            file_name_folder)

    delta_table = \
        DeltaTable(
            absolute_file_name_folder_path)

    # Optimize the DeltaTable
    delta_table.optimize()

    pyarrowtable = \
        delta_table.to_pyarrow_table().to_pandas()

    print(
        'Data was read from folder: ' + str(os.sep.join(absolute_file_name_folder_path.split(os.sep)[7:])))

    return \
        pyarrowtable


def __export_pyarrow_table_to_parquet(
        output_root_folder: Folders,
        pyarrow_table,
        stage_root_folder_name: str,
        stage: str,
        parquet_folder_name: str):
    builder = \
        pyspark.sql.SparkSession.builder.appName("MyApp")\
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.debug.maxToStringFields", 1000)

    spark_session = \
        configure_spark_with_delta_pip(builder).getOrCreate()

    output_parquet_folder_path = \
        os.path.join(
            output_root_folder.absolute_path_string,
            stage_root_folder_name,
            stage,
            parquet_folder_name)
    
    if os.path.exists(output_parquet_folder_path):
        shutil.rmtree(
            output_parquet_folder_path)

        os.mkdir(
            output_parquet_folder_path)

    else:
        os.makedirs(
            output_parquet_folder_path)

    # pyarrow.parquet.write_table(
    #     pyarrow_table,
    #     output_parquet_folder_path)

    # pyarrow_table = \
    #     pyarrow_table.astype(str)

    pyarrow_table = \
        pyarrow_table.head(1000)

    spark_dataframe = \
        spark_session.createDataFrame(
            pyarrow_table.fillna(str()))

    spark_dataframe = \
        spark_dataframe.coalesce(1)

    # pyarrow_table.to_parquet(
    #     output_parquet_folder_path,
    #     engine='pyarrow',
    #     partition_cols=[],
    #     compression='snappy')

    # spark_dataframe.write.save(
    #     path=output_parquet_folder_path + os.sep + 'test',
    #     # format="delta",
    #     # mode='overwrite',
    #     overwriteSchema=True)

    spark_dataframe.write.format("delta").mode("overwrite").save(output_parquet_folder_path)

    print('*'*25 + 'DONE: ' + str(output_parquet_folder_path))
