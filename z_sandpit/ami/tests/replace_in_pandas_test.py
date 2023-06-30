import numpy as np
import pandas as pd
from sat_workflow_source.b_code.etl_processes_wrapper.common_knowledge.satf_constants import DEFAULT_NULL_VALUE


df = pd.DataFrame({
    'A': ['foo', '', 'baz', 'qux', ''],
    'B': ['', 'bar', None, np.nan, 'foo'],
    'C': ['baz', 'qux', 'foo', '', 'bar']
})

# replace all empty strings with 'EMPTY'
cleaned_df = \
    df.replace(
        str(),
        DEFAULT_NULL_VALUE)

print(df)

print(cleaned_df)
