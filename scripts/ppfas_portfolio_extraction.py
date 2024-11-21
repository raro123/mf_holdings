import pandas as pd
import numpy as np
from pathlib import Path

col_list = ['instrument', 'isin','industry', 'quantity', 'value', 'percentage_of_net_assets','ytm']
equity_domestic = ['Equity & Equity related']
arbitrage = ['Arbitrage','Arbitrage & Special Situations']
unlisted = ['(b) Unlisted']
equity_foreign = ['Equity & Equity related Foreign Investments']
derivatives = ['Derivatives']
money_market = ['Money Market Instruments']
options = ['Index / Stock Options']
futures = ['Index / Stock Futures']
end_note = ['Notes:']
table_extract_combos = [(equity_domestic, arbitrage),(arbitrage, unlisted), (equity_foreign, options), (options,money_market), (money_market, futures),(futures,end_note)]

def extract_clean_assets_from_fund_single_file(fund_name: str, file_path: Path, table_extract_combos:list):
    df = pd.read_excel(file_path)
    statement_period = extract_statement_period(df)
    df_dict_raw = extract_all_raw_tables(df, table_extract_combos)
    df_list_clean = clean_all_raw_tables(df_dict_raw)
    combined_df = pd.concat(df_list_clean).assign(statement_period=statement_period).reset_index(drop=True)
    return combined_df
    
    

def extract_statement_period(df):
    return df[df.iloc[:,1].astype('str').str.contains('Monthly Portfolio Statement')].iloc[:,1].values[0]

def get_idx_from_instrument_text(df, text, column=1):
    #return df[df.iloc[:, column]==text].index[0]
    return df[df.iloc[:, column].isin(text)].index[0]

def get_raw_table(df, start_idx_text, end_idx_text):
    start_idx = get_idx_from_instrument_text(df, start_idx_text)
    end_idx = get_idx_from_instrument_text(df, end_idx_text)
    table_df = df.iloc[start_idx:end_idx-1, :]
    return table_df


def extract_all_raw_tables(df, table_extract_combos):
    df_dict = {}
    for start, end in table_extract_combos:
        try:
            df_dict[start[0]] = get_raw_table(df, start, end)
        except Exception as e:
            print(f'Error extracting table for {start} to {end} with error {e}')
    return df_dict


def clean_raw_table(df):
    if len(df.iloc[:,1:-3].columns) != 7:
        df = df.assign(unknown = np.nan) # if only 6 columns, add a column of NaNs so that indexing below doesnt remove any useful column
    clean_df = (df
                .iloc[:,1:-3]
                .pipe(rename_columns, col_list)
                .query('(~quantity.isnull())|(~ytm.isnull())')
                .assign(
                        quantity = lambda x: x.quantity.astype(float),
                        value = lambda x: x.value.astype(float),
                        percentage_of_net_assets = lambda x: x.percentage_of_net_assets.astype('string').str.replace('$', '').str.replace('%', '').astype(float),
                        ytm = lambda x: x.ytm.astype(float)
                    )
                )
    return clean_df

def rename_columns(df, col_list):
    df.columns = col_list
    return df


def clean_all_raw_tables(df_dict):
    clean_df_list = []
    for key,df in df_dict.items():
        try:
            print(key)
            df = clean_raw_table(df).assign(table_name=key)
            clean_df_list.append(df)
        except Exception as e:
            print(e)
            continue
    return clean_df_list


def read_fund_files_path(fund_code: str) -> pd.DataFrame:
    """
    Process all Excel files for a given fund code.
    
    Args:
        fund_code (str): The fund code to process (e.g. 'PPFAS')
        
    Returns:
        pd.DataFrame: Combined processed data from all Excel files
    """
    try:
        # Get data directory path 
        data_dir = Path('data') / fund_code
        
        if not data_dir.exists():
            raise FileNotFoundError(f"Data directory not found: {data_dir}")
            
        # Get all Excel files
        excel_files = list(data_dir.glob('*.xls*'))
        
        if not excel_files:
            print(f"No Excel files found in {data_dir}")
            
        return excel_files
        
    except Exception as e:
        print(f"Error processing files: {e}")
        return []
    
    
def run_ppfas_extraction_flow():
    comb_df_list = []
    file_paths = read_fund_files_path('ppfas')
    for f in file_paths:
        try:
            df = extract_clean_assets_from_fund_single_file('ppfas', f, table_extract_combos)
            comb_df_list.append(df)
            print(f'{f}- {df.percentage_of_net_assets.sum()}')
        except Exception as e:
            print(f'Error processing file {f} with error {e}')
    
    combined_all_df = pd.concat(comb_df_list)
    print(combined_all_df.groupby('statement_period').percentage_of_net_assets.sum())
    
    combined_all_df.to_csv('output/ppfas_portfolio.csv', index=False)
    

if __name__ == '__main__':
    run_ppfas_extraction_flow()
    
    
        
        