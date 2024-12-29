import pandas as pd

def calculatePortfolioPD (list_ownfirmoutputs_climatepds):
    
    df_combined = pd.concat(list_ownfirmoutputs_climatepds)    

    df_edf = df_combined.sort_values(by=["entityId","RiskType","Scenario","year"])
    df_edf[['year','pd']] = df_edf[['year','pd']].apply(pd.to_numeric)
    df_edf['lag.pd'] = df_edf.groupby(["entityId","RiskType","Scenario"])['pd'] .shift(1)
    df_edf['forward_pd'] = 100 * (1-((1-(df_edf['pd']/100)) ** df_edf['year'])/ ((1-(df_edf['lag.pd']/100))** (df_edf['year']-1)))
    
    df_edf_bl = df_edf[df_edf['RiskType']=='baseline'][['entityId','year','forward_pd']].copy()
    df_edf_bl.rename(columns={'forward_pd':'forward_pd_baseline'}, inplace=True)
    
    df_edf1 = pd.merge(df_edf, df_edf_bl, on=['entityId','year'])
    df_edf1['change of forward_pd from baseline'] = df_edf1['forward_pd']/df_edf1['forward_pd_baseline'] - 1
    df_edf1 = df_edf1.sort_values(by=["entityId","RiskType","Scenario","year"])
    
    df_portfolio_edf = df_edf1.groupby(['Scenario','RiskType','year'])[['pd','forward_pd','change of forward_pd from baseline']].median()


    return df_portfolio_edf


