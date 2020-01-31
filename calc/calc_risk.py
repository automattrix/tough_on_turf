

def score(d, tmp_df):
    df = tmp_df['data']

    # arbitrary risk score
    df['score'] = ((df['dirvalue'] + df['vel_avg'] + df['vel_avg_change']) / df['timesum']) + \
            (df['rel_ang_diff_change'] / df['timesum']) + \
            ((df['rel_ang_diff'] - abs(df['rel_ang_diff_change'])) / df['timesum'])
    return df
