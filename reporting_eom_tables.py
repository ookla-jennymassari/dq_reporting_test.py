import pandas as pd

def process_eom_data(curr_test, comp_test, on_column, drop_columns, measure_name, curr_col, comp_col):

    eom_df = pd.merge(curr_test, comp_test, on=on_column, how='outer')
    eom_df = eom_df.drop(columns=drop_columns)

    eom_df['delta'] = eom_df[curr_col] - eom_df[comp_col]

    eom_df['pct_change'] = round(((eom_df[curr_col] - eom_df[comp_col]) / eom_df[comp_col]) * 100, 2)

    eom_df['measure'] = measure_name

    eom_df = eom_df[['measure', on_column, curr_col, comp_col, 'delta', 'pct_change']]

    eom_df = eom_df.rename(columns={
        curr_col: 'current',
        comp_col: 'comparison',
        'delta': 'delta',
        'pct_change': 'pct_change'
    })

    return eom_df

