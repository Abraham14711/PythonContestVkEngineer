#!/usr/bin/env python3
import sys
import pandas as pd
from datetime import datetime, timedelta

def load_data_for_last_7_days(date:str)->pd.DataFrame:

    target = datetime.strptime(date, "%Y-%m-%d")
    
    dates = [
        (target - timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range(7)
    ]
    
    data_frames = [
        pd.read_csv(filepath_or_buffer=f'input/{curr_date}.csv', header=None, names=['email', 'action', 'date'])
        for curr_date in dates
    ]

    return pd.concat(data_frames)

def count_actions(df:pd.DataFrame)->pd.DataFrame:
    action_counts = df.groupby(['email', 'action']).size().unstack(fill_value=0)

    for action in ['CREATE', 'READ', 'UPDATE', 'DELETE']:
        if action not in action_counts:
            action_counts[action] = 0

    result_df = action_counts.reset_index().rename(columns={
        'CREATE': 'create_count',
        'READ': 'read_count',
        'UPDATE': 'update_count',
        'DELETE': 'delete_count'
    })
    
    return result_df

date = sys.argv[1]
result_df = count_actions(load_data_for_last_7_days(date))
result_df.to_csv(f'output/{date}.csv', index=False)