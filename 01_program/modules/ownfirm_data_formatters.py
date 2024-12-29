import pandas as pd
import pandasql as psql
# pip install pandasql

# %%
def num_to_str(var):
    return str(var) if isinstance(var, (int, float)) else var

# %%
def isAsync(df_cpdproperties):
    # return boolean flag of asyncResponse indicator
    return df_cpdproperties.set_index('Parameter')['Value'].to_dict()['asyncResponse']    

# %%
def getInputTable(dict_df):
    df = dict_df['Input Table'].copy(deep=True)
    
    # keep the original row seq number
    # df.insert(0, 'orgRowNo', df.reset_index().index.map(lambda x: f'row{x+1:06d}')) 
    
    # remove the row if None, NaN, Blank value in entityID
    df = df[(df['entityId'].notna()) & (df['entityId'] != '')]
    """
    # Obsolete: leading zero seems not be a proper way to format entityId
    df['entityId'] = df['entityId'].apply(lambda x: 
                            f"{int(float(x)):06d}" if isinstance(x, (int, float)) and (x >0 and x <= 99999) 
                            else (f"{int(float(x)):09d}" if isinstance(x, (int, float)) and (x >= 100000 and x <= 999999) else x)
                            )
    """     
    return df

# %%
def getCPDProperties(dict_df):
    df = dict_df['Climate Adjusted PD properties'].copy(deep=True)
    # Use a lambda function to convert "TRUE" and "FALSE" to boolean values for the 'Value' column
    df['Value'] = df['Value'].apply(lambda x: str(x).upper() == 'TRUE' if str(x).upper() in ('TRUE', 'FALSE') else x)   
    
    return df

# %%
def getDeliverableControl(dict_df):
    df = dict_df['Deliverables Control'].copy(deep=True)

    dict_dcontrol = {}
    for index, row in df.iterrows():
        deliverable = row['Deliverable']
        value = row['Value']
        dict_dcontrol[deliverable] = value       
    
    return dict_dcontrol

# %%
def genAPIInput_Entity(df_inputtable):
    # generate json to check Entity in Moodys API, public firms would not be missing value.
    data = {"queries": [{"pid": pid} for pid in df_inputtable['entityId']]}
    json_str = str(data).replace("'", '"')
    return json_str
    
# %%
def extractAPIOutput_Entity(api_response):
    dict_apioutput = { 
        'count':api_response.json()['total'],
        'data': None
    }
    if api_response.status_code == 200 and dict_apioutput['count'] > 0:
        df = pd.json_normalize(api_response.json(), record_path="entities")
        dict_apioutput['data'] = df
    return dict_apioutput

# %%
def update_EntitySearch_Result(df_inputtable, dict_apioutput):
    if dict_apioutput['data'] is not None:
        df_delta1 = dict_apioutput['data'][['entityId','internationalName']].copy(deep=True)
        df_delta2 = dict_apioutput['data'][['pid','internationalName']].copy(deep=True)
        df_delta3 = dict_apioutput['data'][['identifierOrbis','internationalName']].copy(deep=True)
        df_delta4 = dict_apioutput['data'][['identifierBvd','internationalName']].copy(deep=True)
        df_delta2.rename(columns={'pid':'entityId'}, inplace=True)   
        df_delta3.rename(columns={'identifierOrbis':'entityId'}, inplace=True)   
        df_delta4.rename(columns={'identifierBvd':'entityId'}, inplace=True)   
    
        query = """
            SELECT COALESCE(
                    df_delta1.'internationalName',
                    df_delta2.'internationalName',
                    df_delta3.'internationalName',
                    df_delta4.'internationalName') AS 'internationalName',
                df_src.*
            FROM df_inputtable
            LEFT JOIN df_delta1 ON df_src.'entityId' = df_delta1.'entityId' 
            LEFT JOIN df_delta2 ON df_src.'entityId' = df_delta2.'entityId' 
            LEFT JOIN df_delta3 ON df_src.'entityId' = df_delta3.'entityId' 
            LEFT JOIN df_delta4 ON df_src.'entityId' = df_delta4.'entityId' 
            
        """    
        df_updatedinputtable = psql.sqldf(query,locals())
    else:
        df_updatedinputtable = df_inputtable
        
    return df_updatedinputtable

# %%
def prepareCoreAPIInputs(dict_apiinput_header, df_inputtable):

    list_apiinputs_solo = []
    list_apiinputs_consolidated = []
    
    # Create an empty list to store entities
    list_entities_consolidated = []
    for index, row in pd.DataFrame(df_inputtable).iterrows():
        """
        # debug
        from pandas import NaT
        row = df_inputtable.iloc[2]
        print (type(row["financialStatementDate"]))
        if issubclass(type(row["financialStatementDate"]) , type(pd.NaT)):
            print ("AAA")
        if row["financialStatementDate"] == np.isnat(np.datetime64("NaT")):
        """
        if row["firmStatus"] !="Public":
            entity = {
                # compulsory fields
                "entityId": num_to_str(row["entityId"]),
                "entityName": num_to_str(row["entityName"]),
                "qualitativeInputs": {
                    "regionDetails": [{
                            "primaryCountry": row["primaryCountry"],
                            "primaryCountryWeight": row["countryWeight"] if row["countryWeight"] is not None else 1
                    }],
                    "industriesDetails": [{
                            "primaryIndustryClassification": row["EDF-XIndustryClass"],
                            "primaryIndustry": row["EDF-XIndustryCode"],
                            "industryWeight": row["EDF-XIndustryWeight"] if row["EDF-XIndustryWeight"] is not None else 1
                    }]
                },
                "quantitativeInputs": {}
            }
            # nice to have inputs
            if row["PD"] is not None:
                entity["pd"] = row["PD"] if row["PD"] <= 1 else 1
            if row["impliedRating"] is not None:
                entity["impliedRating"] = row["impliedRating"]
    
            if row["financialStatementDate"] is not None and not issubclass(type(row["financialStatementDate"]) , type(pd.NaT)):
                if isinstance(row["financialStatementDate"], str) :
                    entity["financialStatementDate"] = row["financialStatementDate"][:10]
                else:
                    entity["financialStatementDate"] = row["financialStatementDate"].strftime("%Y-%m-%d")
            if row["asOfDate"] is not None and not issubclass(type(row["asOfDate"]) , type(pd.NaT)):
                if isinstance(row["asOfDate"], str) :
                    entity["asOfDate"] = row["asOfDate"][:10]
                else:
                    entity["asOfDate"] = row["asOfDate"].strftime("%Y-%m-%d")
                
            if row["netSales"] is not None:
                if isinstance(row["netSales"], str) :
                    entity["quantitativeInputs"]["netSales"] = float(row["netSales"]) 
                else: 
                    entity["quantitativeInputs"]["netSales"] = row["netSales"] 
            if row["totalAssets"] is not None:
                if isinstance(row["totalAssets"], str) :
                    entity["quantitativeInputs"]["totalAssets"] = float(row["totalAssets"])  
                else: 
                    entity["quantitativeInputs"]["totalAssets"] = row["totalAssets"]
            
        elif row["firmStatus"] == "Public":
            entity = {"entityId": num_to_str(row["entityId"])}
        
        dict_apiinput_header_solo = dict(dict_apiinput_header)
        dict_apiinput_header_solo["entities"] = [entity]
        list_apiinputs_solo.append(dict_apiinput_header_solo)
        list_entities_consolidated.append(entity)
            
    dict_apiinput_header_consolidated = dict(dict_apiinput_header)
    dict_apiinput_header_consolidated["entities"] = list_entities_consolidated
    list_apiinputs_consolidated.append(dict_apiinput_header_consolidated)

    return list_apiinputs_solo, list_apiinputs_consolidated

# %%
def genListOfAPIInput_ClimatePDs(df_cpdproperties, df_inputtable, logger):

    logger.info("Begin to prepare API inputs of climate-adjusted PDs ...") if logger is not None else None 
    
    # Create the final result dictionary
    dict_apiinput_header = {}
    for index, row in df_cpdproperties.iterrows():
        parameter = row['Parameter']
        value = row['Value']
        if parameter == 'asyncResponse':
            dict_apiinput_header[parameter] = value
        elif parameter == 'scenarioCategory':
            dict_apiinput_header['scenarios'] = {"scenarioCategory": value}
        elif parameter in ('transition', 'physical', 'combined'):
            if 'riskTypes' not in dict_apiinput_header:
                dict_apiinput_header['riskTypes'] = {}
            dict_apiinput_header['riskTypes'][parameter] = value
        elif parameter in ('resultDetailMain', 'resultDetailTransition'):
            if 'includeDetail' not in dict_apiinput_header:
                dict_apiinput_header['includeDetail'] = {}
            dict_apiinput_header['includeDetail'][parameter] = value


    list_apiinputs_solo, list_apiinputs_consolidated = prepareCoreAPIInputs(dict_apiinput_header, df_inputtable)

    list_apiinputs = []
    if isAsync(df_cpdproperties) :    
        list_apiinputs = list_apiinputs_consolidated
    else:
        list_apiinputs = list_apiinputs_solo
        
    logger.info("Finish preparing API inputs of climate-adjusted PDs") if logger is not None else None 

    return isAsync(df_cpdproperties), list_apiinputs


# %%
def genListOfAPIInput_Reports(df_cpdproperties, df_inputtable, logger):
    logger.info("Begin to prepare API inputs for accessing Moody's predefined reports ...") if logger is not None else None 

    str_scencat = df_cpdproperties[df_cpdproperties['Parameter'] == 'scenarioCategory']['Value'].values[0]
    dict_apiinput_header = {"reportType": "climate", "reportFormat": "pdf", "scenarioCategory":[str_scencat], "entities":None}
    list_apiinputs_solo, list_apiinputs_consolidated = prepareCoreAPIInputs(dict_apiinput_header, df_inputtable)
        
    logger.info("Finish preparing API inputs of climate-adjusted PDs") if logger is not None else None 

    return list_apiinputs_solo


# %%
def genAPIInput_ESG(df_cpdproperties, df_inputtable, logger):
    logger.info("Begin to prepare API inputs for ESG Score Predictor ...") if logger is not None else None 

    list_apiinputs_esg = []
    for index, row in pd.DataFrame(df_inputtable).iterrows():
        entity = {"batchResponseIdentifier": num_to_str(row["entityId"])}
        if row["periodYear"] is not None:
            if isinstance(row["periodYear"], str) :
                entity["periodYear"] = int(row["periodYear"]) 
            else:
                entity["periodYear"] = row["periodYear"]
        if row["regionClassification"] is not None:
            entity["regionClassification"] = row["regionClassification"]
        if row["regionCode"] is not None:
            entity["regionCode"] = row["regionCode"]
        if row["ESGIndustryClass"] is not None:
            entity["industryClassification"] = row["ESGIndustryClass"]
        if row["ESGIndustryCode"] is not None:
            entity["industryCode"] = row["ESGIndustryCode"]
        if row["employeeCount"] is not None:
            if isinstance(row["employeeCount"], str) :
                entity["employeeCount"] = int(row["employeeCount"]) 
            else:
                entity["employeeCount"] = row["employeeCount"]
        if row["assetTurnover"] is not None:
            if isinstance(row["assetTurnover"], str) :
                entity["assetTurnover"] = float(row["assetTurnover"])  
            else: 
                entity["assetTurnover"] = row["assetTurnover"]
        if row["totalAssets"] is not None:
            if isinstance(row["totalAssets"], str) :
                entity["totalAssets"] = float(row["totalAssets"])  
            else: 
                entity["totalAssets"] = row["totalAssets"]
        if row["carbonIntensity"] is not None:
            entity["carbonIntensity"] = row["carbonIntensity"]
        
        list_apiinputs_esg.append(dict(entity)) 
    
    logger.info("Finish preparing API inputs for ESG Score Predictor") if logger is not None else None 
    
    return list_apiinputs_esg

# %%
def extractAPIOutput_ClimatePDs(list_responses_climatepds, logger):
    logger.info("Begin to flatten API outputs of climate-adjusted PDs ...") if logger is not None else None 
    
    list_ownfirmoutputs_climatepds=[]
    count_loop = 0    
    for response in list_responses_climatepds:
        count_loop +=1
        if response.status_code == 200:
            json_data = response.json()
            list_scenarocat = [json_data["scenarioCategory"]]
            list_risktypesmatches = ['combinedRisk','physicalRisk','transitionRisk']
   
            df_entity = pd.DataFrame()
            for entity in json_data["entities"]:
                # if errorMessage is found, log it and go to next iteration
                if "errorMessage" in entity:
                    list_ownfirmoutputs_climatepds.append(None)
                    logger.info(f"-> Error Message: {entity['errorMessage']} showed in entity ID: {entity['entityId']} at iteration #{count_loop} of Total #{len(list_responses_climatepds)}") if logger is not None else None 
                else: 
                    
                    list_entityid = [entity["entityId"],entity["asOfDate"],entity["isfin"]]
    
                    for risktype, value in entity.items():
                        if isinstance(value, dict) and any([x in risktype for x in list_risktypesmatches]):
                            for scenario in value.keys():
                                scenario_pd = pd.Series(value[scenario]["pd"]).reset_index()[0]
                                scenario_ir = pd.Series(value[scenario]["impliedRating"]).reset_index()[0]
                                df_pd_ir = pd.concat([scenario_pd,scenario_ir], axis=1, keys=["pd","ir"])
                                df_pd_ir["year"] = df_pd_ir.reset_index().index+1
                                
                                len_pd_ir = len(df_pd_ir)
                                list_prefix = list_scenarocat+list_entityid+[risktype, scenario]
                                df_prefix = pd.DataFrame([list_prefix]*len_pd_ir) 
                                df_eachrisktype = pd.concat([df_prefix,df_pd_ir],axis=1)
                                
                                df_entity = pd.concat([df_entity, df_eachrisktype], ignore_index=True, sort=False)
                                
                        elif isinstance(value, dict) and risktype == 'baseline':
                            baseline_pd = pd.Series(value["pd"]).reset_index()[0]
                            baseline_ir = pd.Series(value["impliedRating"]).reset_index()[0]
                            df_pd_ir = pd.concat([baseline_pd,baseline_ir], axis=1, keys=["pd","ir"])
                            df_pd_ir["year"] = df_pd_ir.reset_index().index+1
                               
                            len_pd_ir = len(df_pd_ir)
                            list_prefix = list_scenarocat+list_entityid+["baseline","baseline"]
                            df_prefix = pd.DataFrame([list_prefix]*len_pd_ir) 
                            df_baseline = pd.concat([df_prefix,df_pd_ir],axis=1)
                                
                            df_entity = pd.concat([df_entity, df_baseline], ignore_index=True, sort=False)
                        else:
                            None
                            #print("Do nothing")
                            
                    # Create a DataFrame
                    df_entity.columns = ["scenarioCategory", "entityId", "asOfDate", "isfin", "RiskType", "Scenario", "pd", "impliedRating", "year"]
                    list_ownfirmoutputs_climatepds.append(df_entity.copy(deep=True))
                    logger.info(f"-> Flatten response data at iteration #{count_loop} of Total #{len(list_responses_climatepds)}") if logger is not None else None 
            
            # end of for loop
        else:
            list_ownfirmoutputs_climatepds.append(None)
            logger.info(f"-> No response data (i.e. status code != 200) at iteration #{count_loop} of Total #{len(list_responses_climatepds)}") if logger is not None else None 
            
    logger.info("Finish flattening API outputs of climate-adjusted PDs") if logger is not None else None 
            
    return list_ownfirmoutputs_climatepds

# %%
def exportAPIOutput_ClimatePDs(path_export, list_ownfirmoutputs_climatepds, logger):
    logger.info("Begin to export API outputs of climate-adjusted PDs to XLSX format ...") if logger is not None else None 
    count_loop = 0    
    for ownfirmoutput in list_ownfirmoutputs_climatepds:
        count_loop +=1
        if ownfirmoutput is not None:
            list_cols= ownfirmoutput.columns.tolist() 
            str_exportfilename = "climate_pd_"+ownfirmoutput['entityId'].iloc[0]+".xlsx"
            ownfirmoutput.to_excel(path_export+"/"+str_exportfilename ,sheet_name='climatePD', columns=list_cols)
            logger.info(f"-> data is exported at iteration #{count_loop} of Total #{len(list_ownfirmoutputs_climatepds)}") if logger is not None else None 

    df_combined = pd.concat(list_ownfirmoutputs_climatepds)    
    df_combined.to_excel(path_export+"/_combined_all.xlsx" ,sheet_name='climatePD', columns=df_combined.columns.tolist())

    logger.info("Finish exporting API outputs of climate-adjusted PDs to XLSX format") if logger is not None else None 
    return 

def exportPortfolioPDs(path_export, df_portfolio_edf, logger):
    logger.info("Begin to export Portfolio PDs to XLSX format ...") if logger is not None else None 
    
    df_portfolio_edf.to_excel(path_export+"/_PortfolioPD.xlsx" ,sheet_name='Portfolio_PD' )

    logger.info("Finish exporting Portfolio PDs to XLSX format") if logger is not None else None 
    return 

# %%
def genAPIInput_TransRiskIndustry(df_cpdproperties, df_inputtable, logger):

    logger.info("Begin to prepare API inputs of Transition Risk Drivers for Industry ...") if logger is not None else None
    
    str_scencat = df_cpdproperties[df_cpdproperties['Parameter'] == 'scenarioCategory']['Value'].values[0]
    filtered_df = df_inputtable[df_inputtable['EDF-XIndustryClass'].isin(['NDY', 'GCAM'])]
    list_industry_code = filtered_df['EDF-XIndustryCode'].unique().tolist()
    str_industry_codes = ",".join(list_industry_code)
    
    dict_apiinputs_industry = {
        "scenarioCategory":str_scencat,
        "industry":str_industry_codes
    }
    logger.info("Finish preparing API inputs of Transition Risk Drivers for Industry") if logger is not None else None

    return dict_apiinputs_industry

# %%
def extractAPIOutput_TransRiskIndustry(obj_responses_industry, logger):
    logger.info("Begin to flatten API outputs of Transition Risk Drivers for Industry ...") if logger is not None else None 
    
    flattened_data = []
    for scenario, entries in obj_responses_industry.json().items():
        for entry in entries:
            flattened_entry = {"scenario": scenario}
            for key, value in entry.items():
                flattened_entry[key] = str(value)
            flattened_data.append(flattened_entry)
            
    df_ownfirmoutputs_industry = pd.DataFrame(flattened_data).sort_values(by=['industry', 'scenario','year']).reset_index(drop=True)

    logger.info("Finish flattening API outputs of Transition Risk Drivers for Industry") if logger is not None else None 
    
    return df_ownfirmoutputs_industry

# %%
def exportAPIOutput_TransRiskIndustry(path_export, df_ownfirmoutputs_industry, logger):
    logger.info("Begin to export API outputs of Transition Risk Drivers for Industry to XLSX format ...") if logger is not None else None 
    
    if df_ownfirmoutputs_industry is not None:
        list_cols= df_ownfirmoutputs_industry.columns.tolist() 
        str_exportfilename = "transition_risk_drivers_for_industry.xlsx"
        df_ownfirmoutputs_industry.to_excel(path_export+"/"+str_exportfilename ,sheet_name='transition_risk', columns=list_cols)

    logger.info("Finish exporting API outputs of Transition Risk Drivers for Industry to XLSX format") if logger is not None else None 
    return None

# %%
def genAPIInput_TransRiskRegion(df_cpdproperties, df_inputtable, logger):

    logger.info("Begin to prepare API inputs of Transition Risk Drivers for Region ...") if logger is not None else None
    
    str_scencat = df_cpdproperties[df_cpdproperties['Parameter'] == 'scenarioCategory']['Value'].values[0]
    filtered_df = df_inputtable[df_inputtable['EDF-XIndustryClass'].isin(['NDY', 'GCAM'])]
    filtered_df['regionIndustry'] = filtered_df.apply(lambda row: f"({row['primaryCountry']},{row['EDF-XIndustryCode']})", axis=1)
    list_regionIndustry_code = filtered_df['regionIndustry'].unique().tolist()
    str_regionIndustry_codes = ",".join(list_regionIndustry_code)
    
    dict_apiinputs_region = {
        "scenarioCategory":str_scencat,
        "regionIndustry":str_regionIndustry_codes
    }
    logger.info("Finish preparing API inputs of Transition Risk Drivers for Region") if logger is not None else None

    return dict_apiinputs_region

# %%
def extractAPIOutput_TransRiskRegion(obj_responses_region, logger):
    logger.info("Begin to flatten API outputs of Transition Risk Drivers for Region ...") if logger is not None else None 
    
    flattened_data = []
    for scenario, entries in obj_responses_region.json().items():
        for entry in entries:
            flattened_entry = {"scenario": scenario}
            for key, value in entry.items():
                flattened_entry[key] = str(value)
            flattened_data.append(flattened_entry)
            
    df_ownfirmoutputs_region = pd.DataFrame(flattened_data).sort_values(by=['region', 'industry', 'scenario','year']).reset_index(drop=True)

    logger.info("Finish flattening API outputs of Transition Risk Drivers for Region") if logger is not None else None 
    
    return df_ownfirmoutputs_region

# %%
def exportAPIOutput_TransRiskRegion(path_export, df_ownfirmoutputs_region, logger):
    logger.info("Begin to export API outputs of Transition Risk Drivers for Region to XLSX format ...") if logger is not None else None 
    
    if df_ownfirmoutputs_region is not None:
        list_cols= df_ownfirmoutputs_region.columns.tolist() 
        str_exportfilename = "transition_risk_drivers_for_region.xlsx"
        df_ownfirmoutputs_region.to_excel(path_export+"/"+str_exportfilename ,sheet_name='transition_risk', columns=list_cols)

    logger.info("Finish exporting API outputs of Transition Risk Drivers for Region to XLSX format") if logger is not None else None 
    return None

# %%
def extractAPIOutput_ESG(response_esg, logger):
    logger.info("Begin to flatten API outputs of ESG ...") if logger is not None else None 
    
    """
    # debug
    apioutput = list_apioutputs[1]
    import json
    str_dump = json.dumps(apioutput,indent=4)
    df = pd.json_normalize(apioutput, record_path=['domainScores'], meta=['globalScores','info', 'inputs'])
    
    df_domain_scores = pd.json_normalize(apioutput, record_path=['domainScores'])
    df_info_inputs = pd.json_normalize(apioutput, record_path=None, meta=['inputs', 'info', 'globalScores']).drop(columns=['domainScores'])

    # Combine the two DataFrames
    new_df = df_domain_scores.join(df_info_inputs).ffill()
    """
    df_ownfirmoutputs_esg = pd.DataFrame()
    for apioutput in response_esg.json():
        df_domain_scores = pd.json_normalize(apioutput, record_path=['domainScores'])
        df_info_inputs = pd.json_normalize(apioutput, record_path=None, meta=['inputs', 'info', 'globalScores']).drop(columns=['domainScores'])
        df_combined = df_domain_scores.join(df_info_inputs).ffill()
        df_ownfirmoutputs_esg = pd.concat([df_ownfirmoutputs_esg , df_combined ], ignore_index=True, sort=False)
    
    logger.info("Finish flattening API outputs of ESG") if logger is not None else None 
    
    return df_ownfirmoutputs_esg 

# %%
def exportAPIOutput_ESG(path_esg, df_ownfirmoutputs_esg, logger):
    logger.info("Begin to export API outputs of ESG to XLSX format ...") if logger is not None else None 
    
    if df_ownfirmoutputs_esg is not None:
        list_cols= df_ownfirmoutputs_esg.columns.tolist() 
        str_exportfilename = "ESG_scores.xlsx"
        df_ownfirmoutputs_esg.to_excel(path_esg+"/"+str_exportfilename ,sheet_name='ESG', columns=list_cols)

    logger.info("Finish exporting API outputs of ESG to XLSX format") if logger is not None else None 
    return None


