import pandas as pd 
import os 

fl_path = '//10.18.4.40/Users/LEPass/Desktop/共有フォルダ/予兆検知/detect_sys/output'
files = sorted(os.listdir(fl_path))

def prepQuantile(df, upper_th, lower_th):
    df = df.copy()
    df['time_d'] = pd.to_datetime(df['time']).dt.floor('D')

    q_upper = df.groupby('time_d')['value'].transform(lambda s: s.quantile(upper_th))
    q_lower = df.groupby('time_d')['value'].transform(lambda s: s.quantile(lower_th))

    mask = (df['value'] < q_upper) & (df['value'] > q_lower)
    return df.loc[mask].drop(columns=['time_d'])

for f_name in files:

    f_paht = os.path.join(fl_path, f_name)

    if '.csv' in f_name:

        df = pd.read_csv(f_paht, encoding='cp932', sep=',') 
        df = df.rename(columns={'X_TIME':'time', 'DURATION_MS':'value'})
        df = prepQuantile(df[['time', 'value']] ,0.99, 0.01)
        print(df)