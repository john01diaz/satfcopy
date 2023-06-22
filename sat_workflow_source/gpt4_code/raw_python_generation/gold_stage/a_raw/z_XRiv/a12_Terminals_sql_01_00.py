import pandas as pd
from pathlib import Path

def load_database_names(csv_file):
    database_names = pd.read_csv(csv_file)
    return database_names['Database_name'].tolist()

def load_terminals(csv_file):
    return pd.read_csv(csv_file)

def filter_terminals(terminals_df, class_filter, database_names):
    filtered_terminals = terminals_df[
        (terminals_df['Class'].isin(class_filter)) &
        (terminals_df['database_name'].isin(database_names))
    ]
    return filtered_terminals

def rank_terminals(filtered_terminals):
    filtered_terminals['Sequence'] = filtered_terminals.groupby(
        ['Parent_Equipment_No', 'Equipment_No']
    )['Marking'].rank(method='dense')
    return filtered_terminals

def _12_Terminals(terminals_csv, database_names_csv):
    class_filter = ['Instrumentation', 'Inst(Shared)', 'Elec(Shared)']
    
    database_names = load_database_names(database_names_csv)
    terminals_df = load_terminals(terminals_csv)
    
    filtered_terminals = filter_terminals(terminals_df, class_filter, database_names)
    
    ranked_terminals = rank_terminals(filtered_terminals)
    
    return ranked_terminals

if __name__ == "__main__":
    terminals_csv = Path("path/to/your/terminals.csv")
    database_names_csv = Path("path/to/your/database_names.csv")
    
    result_df = _12_Terminals(terminals_csv, database_names_csv)
    print(result_df)
