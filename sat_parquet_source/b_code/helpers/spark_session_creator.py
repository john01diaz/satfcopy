from pyspark.sql import SparkSession


def create_pyspark_session(
        session_name_string: str) \
        -> SparkSession:
    spark_session = \
        SparkSession.builder.appName(
            session_name_string) \
        .config("spark.driver.memory", "14g") \
        .config("spark.executor.memory", "14g") \
        .getOrCreate()

    spark_session.sparkContext.setJobGroup(
        groupId=session_name_string,
        description=session_name_string)

    spark_session.conf.set(
        "spark.sql.debug.maxToStringFields",
        "1000")

    # Note: this option exists only for internal test cases.
    # It's not for external use and it is not supposed to be used in user programs.
    spark_session.conf.set(
        'spark.driver.allowMultipleContexts',
        "true")

    return \
        spark_session
