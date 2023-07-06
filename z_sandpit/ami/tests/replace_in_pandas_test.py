from numpy import nan
from pandas import DataFrame
from sat_workflow_source.b_code.etl_processes_wrapper.common_knowledge.satf_constants import B_DEFAULT_ISNULL


dataframe = DataFrame({
    'A': ['foo', '', 'baz', 'qux', ''],
    'B': ['', 'bar', None, nan, 'foo'],
    'C': ['baz', 'qux', 'foo', '', 'bar']
})

cleaned_dataframe = \
    dataframe.replace(
        nan,
        B_DEFAULT_ISNULL,
        regex=True)

print(
    dataframe)

print(
    cleaned_dataframe)
