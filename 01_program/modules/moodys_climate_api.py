# -*- coding: utf-8 -*-
"""
@author:    Philip Yam
@Purpose:   This is a program to handle Moody's Climate Module in EDF-X/ESG API.
@Date:      Sep 14 21:00:00 2023
@Remark:    press F9 to run selection    



Ref links : 
------------------------------------------------------------------------------
https://www.moodysanalytics.com/whitepapers/pa/2023/quantifying-the-impact-of-climate-on-corporate-credit-risk
https://hub.moodysanalytics.com/

Backgroud
------------------------------------------------------------------------------
The Public Firm EDF model is a structural model of credit risk that has been used by financial institutions for more than
30 years. To augment the Public EDF framework to account for climate risk, Moody’s Analytics has developed a methodology to
account for the effect of climate on the underlying drivers of EDF metrics. 
The climate-adjusted model integrates climate scenarios devised by the Network for Greening the Financial System (NGFS)3 
and state-of-the-art data and assessment tools from the Moody’s MESG team to project the physical and transition risk metrics
related to global warming and their impact on credit risk.

Abb: 
    EDF - Expected Default Frequency
    CDEF - Climate-Adjusted EDF
    GCAM - Global Change Analysis Model 

Moody’s Analytics Climate-Adjusted EDF™ framework
------------------------------------------------------------------------------
it provides means for analyzing physical and transition risks’ impact on public companies’ credit risk.

    Framework's models
    ==================
    - The Physical Risk-Adjusted EDF model
    it forecasts both direct and indirect effects of weather and climate events on businesses’ infrastructure, 
    operations, and markets.
    
    - The Transition Risk-Adjusted EDF model 
    it forecasts the risks associated with the transition to a lower carbon economy. To capture the complicated 
    economic drivers affecting firms, we employ an Integrated Assessment Model (IAM) to understand how a given 
    transition future affects sector-level prices, quantities sold, and costs. 

    - The Climate PD Converter 
    it allows users to adjust their own baseline PDs. Based on a firm’s non-climate adjusted probability of 
    default (PD) inputs and other characteristics, the Converter outputs a full set of credit metrics conditional 
    on a range of climate scenarios. 
    
    Functionality
    =============
    - 30-year EDF term structures conditional on a climate scenario: 
        it produces PD term structure that forecasts credit risk over time for each climate scenario.
        
    - Conditional valuation metrics:
        it leverages on conditional PD term structures and adjust these to the physical and transition climate risks
        to produce the climate-adjusted EDF term structure as well as a host of intermediate outputs, such as 
        earnings, assets, and implied ratings. 
    
    - Climate adjustment of user-supplied PDs via the Climate PD Converter:
        this module allows users to input a (unconditional) baseline PD and key characteristics of a custom entity, 
        such as asset value, sales, carbon footprint, and scope 1 and 2 emissions. The tool is useful for running 
        unlisted and private names through the model, understanding generic sectoral and regional risks, and adjusting
        internal or reduced form PD models to account for climate risk.
        
    Modules Flow
    =============
    CDEF 
     | <-- GCAM for generating climate scenarios
     |
     | <-| leverage on MESG firm-specific physical scores to derive firm-level damages
     |   |-------> generating physical risk detail.
     |
     | <-|  leverage GCAM combined with firm-level scope 1 and 2 emissions to generate sectoral and regional outputs.
     |   |-------> generating transition risk detail.
     |
     | <-| from physical risk & transition risk detail
     |   |-------> to adjust firm-level costs.
     |   |-------> combine impact of 2 risks via the path of asset values
     |
     | <-| using these costs and a model of oligopolistic competition
     |   |-------> disaggregate sectoral output to the firm level
     |   |-------> generate joint output & aggregated risks in a portfolio of corporate bonds.
     : 
     :
     :
     | <-| customize inputs such as emissions plus assets and sales from financials (i.e The Climate PD Converter)
     |   |-------> produce climate-adjusted EDFs for both private and public companies
     |
     | ==> final output is a term structure, the corporate probability of default, one of the credit risk metrics.
     
"""
# %%
import pandas as pd
import requests
import json
import time
from pathlib import Path


# %%
# Calculate the path to the `config.json` file in the parent directory
config_path = Path(__file__).resolve().parent.parent / "config.json"
if not config_path.exists():
    raise FileNotFoundError(f"Config file not found: {config_path}")

# Load the JSON configuration
with open(config_path, "r") as f:
    config = json.load(f)

dict_auth = config.get("auth", {})
if not dict_auth:
    raise KeyError("Missing 'auth' section in config.json")
# Now `dict_auth` contains dynamically loaded clientId, clientSecret, and URL

"""
The EDF-X API Climate 
    1. Purpose
        - To retrieve Climate Adjusted PDs
        - To retrieve Transition Risk Drivers for Country and Sector

    2. Types of input:
        - If the company is listed: input identifier, and then it responses climate adjusted analytics.
        - If the company is not listed: the API acts as a climate adjustment calculator based on your input.
"""
dict_ESG_EDFX = {
    'ESG'              : ["https://api.esg.moodysanalytics.com/esgsp/v2/proxyScore","POST"],
    'URL_EDFX'         : ["https://api.edfx.moodysanalytics.com","POST"],
    'entity_single'    : ["/entity/v1/search","POST"],                               # Searching For An Entity - for one company; POST method 
    'entity_batch'     : ["/entity/v1/mapping","POST"],                              # Searching For An Entity - for batch of companies; POST method
    'climate_pds_TPC'  : ["/climate/v2/entities/pds", "POST"],                       # Retrieving Climate Adjusted PDs; GET method; 
                                                                                     # Transition/Physical/Combined Risk are available depend on input parameters.
                                                                                     # It is Recommended to enable args of asyncResponse = true
    'industry_T'       : ["/climate/v2/industry/industryTransitionPaths","GET"],     # Retrieving Industry-level drivers for transition risk; GET method
    'region_T'         : ["/climate/v2/industry/regionTransitionPaths","GET"],       # Retrieving Region-level drivers for transition risk; GET method
    'process_Id'       : ["/edfx/v1/processes","GET"],                               # /{processId}/status or /{processId}/files must be added at the end of this URL.
    'reports'          : ["/edfx/v1/reports","POST"],                                # it allows access to PDF and CSV files.
}

# %%
def getAuth():
    # descriptions of response status code 
    # 200	OK - User Authenticated
    # 401	Unauthorized - Provided credentials are not valid
    # 4XX	Parameter Request Error - The request parameters are incorrect
    # 5XX	Server Error - A problem has been found with the system
    
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'application/json'
    }
    data = {
        'grant_type':     'client_credentials', 
        'scope':          'openid',
        'client_id':      dict_auth['clientId'],
        'client_secret':  dict_auth['clientSecret']
    }
    
    dict_token = {}
    response = requests.post(dict_auth['URL'], headers=headers, data=data)
    rs = response.status_code
    if rs == 200:
        dict_token = json.loads(response.text)
    else:
        print(f"Moody's Authentication failed with status code: {rs}")
    
    return rs, dict_token

# %%
def getResponse(dict_token, info_type, json_data):
    # https://stackoverflow.com/questions/15900338/python-request-post-with-param-data
    # https://realpython.com/python-json/
    """
    Possible Content-Types:   
    ------------------------------------------
    application/json: Used for sending JSON data.
    application/xml: Used for sending XML data.
    text/plain: Used for sending plain text data.
    text/html: Used for sending HTML content.
    application/x-www-form-urlencoded: Used for sending form data as key-value pairs.
    multipart/form-data: Used for sending binary data, typically when uploading files.
    application/pdf: Used for sending PDF documents.
    image/jpeg: Used for sending JPEG images.
    image/png: Used for sending PNG images.
    audio/mpeg: Used for sending MP3 audio files.
    video/mp4: Used for sending MP4 video files.
    application/octet-stream: Used for arbitrary binary data where the type is not known.
    """    
    rs = 000
    url = None
    response = None
    method = None
    
    if info_type == "ESG":
        url = dict_ESG_EDFX[info_type][0]
        method = dict_ESG_EDFX[info_type][1]
    else:
        url = dict_ESG_EDFX["URL_EDFX"][0]+dict_ESG_EDFX[info_type][0]
        method = dict_ESG_EDFX[info_type][1]
    
    if method == "POST":
        header = {"Authorization": "Bearer "+dict_token['id_token'],"Content-Type":"application/json"} 
        response = requests.post(url=url, headers=header, data=json_data)
    else:
        header = {"Authorization": "Bearer "+dict_token['id_token']} 
        response = requests.get(url=url, headers=header, params=json_data)
    rs = response.status_code
    
    return rs, response

# %%
def getDownloadLink(url):
    response = requests.get(url, verify=False)
    return response.status_code, response

# %%
def getProcessResult(dict_token, pid):
    
    header = {"Authorization": "Bearer "+dict_token['id_token']} #do not add content-type
    url_status = dict_ESG_EDFX["URL_EDFX"][0]+dict_ESG_EDFX['process_Id'][0]+"/"+pid+"/status"

    rs = 000
    rs_data = None
    while True:
        response = requests.get(url=url_status, headers=header)
        status = response.json()["status"]
        if status == "Errored":
            rs = 500
            break
        elif status == "Completed":
            url_files = dict_ESG_EDFX["URL_EDFX"][0]+dict_ESG_EDFX['process_Id'][0]+"/"+pid+"/files"
            res = requests.get(url=url_files, headers=header)
            dl_url = res.json()["downloadLink"]
            rs, rs_data = getDownloadLink(url=dl_url)
            break
        else:
            time.sleep(5)
            
    return rs, rs_data

# %%
# --- debug ---
if __name__ == '__main__':
    
    # test case 1 
    # ----------------------------------------------------------------
    info_type="entity_single"
    data = {"query": "Apple Inc"}
    json_data=json.dumps(data,indent=4)
    rs_auth, dict_token = getAuth()
    rs, rs_json = getResponse(dict_token, info_type, json_data)
    dfItem = pd.json_normalize(rs_json.json(), record_path="entities")


    # test case 2 
    # ----------------------------------------------------------------
    info_type="entity_batch"
    data={"queries":[
        {"entityidentifierbvd": "TW23535744"},
        {"pid": "037833"},
        {"isin" : "KR7000270009"},
        {"cusip" : "594918"},
        {"lei" : "7LTWFZYICNSX8D621K86"}
      ]
    } 
    json_data=json.dumps(data,indent=4)
    rs_auth, dict_token = getAuth()
    rs, rs_json = getResponse(dict_token, info_type, json_data)
    dfItem = pd.json_normalize(rs_json.json(), record_path="entities")


    # test case 3 
    # ----------------------------------------------------------------
    info_type="climate_pds_TPC"
    json_string = '''
        {  
            "asyncResponse": false,
            "scenarios" : {  
                    "scenarioCategory":"NGFS"  
            },  
            "riskTypes" : {  
                    "transition" : true,  
                    "physical" : true,  
                    "combined" : true  
                },  
            "includeDetail": {  
                    "resultDetailMain": false,  
                    "resultDetailTransition": false  
                },  
            "entities": [  
                {  
                    "entityId": "W48258"  
                },  
                {  
                    "entityName": "UNIQLO",  
                    "entityId": "JP9250001001451",  
                    "financialStatementDate": "2021-08-31",  
                    "carbonEmissionDate": "2021-12-01",  
                    "asOfDate": "2021-12-01",  
                    "qualitativeInputs": {  
                        "regionDetails": [  
                            {  
                                "primaryCountry": "JPN",  
                                "primaryCountryWeight": 1  
                            }  
                        ],  
                        "industriesDetails": [  
                            {  
                                "primaryIndustryClassification": "NDY",  
                                "primaryIndustry": "N18",  
                                "industryWeight": 1  
                            }  
                        ]  
                    },  
                    "pd": 0.001788,  
                    "impliedRating":"A3",  
                    "quantitativeInputs": {  
                        "totalAssets": 512.649,  
                        "netSales": 828.886,  
                        "scope1Emission": 77774.65,  
                        "scope2Emission": 15475.56  
                    },
                    "physicalRiskScore": {
                        "physicalRiskScoreOverwrite": 36
                    }  
                }  
            ]  
        }    
    '''
    # when set asyncResponse = true, entities detail would be removed but only have processID info.
    data=json.loads(json_string)
    json_data = json.dumps(data, indent=4, ensure_ascii=False)
    rs_auth, dict_token = getAuth()
    rs, rs_json = getResponse(dict_token, info_type, json_data)
    dfItem = pd.json_normalize(rs_json.json(), record_path="entities")


    # test case 4 
    # ----------------------------------------------------------------
    info_type="industry_T"
    #dict_data = {"scenarioCategory":"NGFS3","industry":"N19"} #cannot use json string or
    dict_data = {"scenarioCategory":"NGFS3","industry":"N19,G23"} #cannot use json string or
    rs_auth, dict_token = getAuth()
    rs_data, rs_json = getResponse(dict_token, info_type, dict_data)
    
    dl_url = rs_json.json()["downloadLink"]
    rs, dl =  getDownloadLink(dl_url)
    # https://towardsdatascience.com/all-pandas-json-normalize-you-should-know-for-flattening-json-13eae1dfb7dd
    flattened_data = []
    for scenario, entries in dl.json().items():
        for entry in entries:
            flattened_entry = {"scenario": scenario}
            for key, value in entry.items():
                flattened_entry[key] = str(value)
            flattened_data.append(flattened_entry)
    df = pd.DataFrame(flattened_data)
    
    
    # test case 5 
    # ----------------------------------------------------------------
    info_type="region_T"
    dict_data = {"scenarioCategory":"NGFS3","regionIndustry":"(POL,N01)"} #cannot use json string or
    rs_auth, dict_token = getAuth()
    rs_data, rs_json = getResponse(dict_token, info_type, dict_data)
    
    dl_url = rs_json.json()["downloadLink"]
    rs, dl =  getDownloadLink(dl_url)
    # https://towardsdatascience.com/all-pandas-json-normalize-you-should-know-for-flattening-json-13eae1dfb7dd
    flattened_data = []
    for scenario, entries in dl.json().items():
        for entry in entries:
            flattened_entry = {"scenario": scenario}
            for key, value in entry.items():
                flattened_entry[key] = str(value)
            flattened_data.append(flattened_entry)
    df = pd.DataFrame(flattened_data)

    
    # test case 6 
    # ----------------------------------------------------------------
    info_type="climate_pds_TPC"
    json_string = '''
        {  
            "asyncResponse": true,
            "scenarios" : {  
                    "scenarioCategory":"NGFS"  
            },  
            "riskTypes" : {  
                    "transition" : true,  
                    "physical" : true,  
                    "combined" : true  
                },  
            "includeDetail": {  
                    "resultDetailMain": false,  
                    "resultDetailTransition": false  
                },  
            "entities": [  
                {  
                    "entityId": "W48258"  
                },  
                {  
                    "entityName": "UNIQLO",  
                    "entityId": "JP9250001001451",  
                    "financialStatementDate": "2021-08-31",  
                    "carbonEmissionDate": "2021-12-01",  
                    "asOfDate": "2021-12-01",  
                    "qualitativeInputs": {  
                        "regionDetails": [  
                            {  
                                "primaryCountry": "JPN",  
                                "primaryCountryWeight": 1  
                            }  
                        ],  
                        "industriesDetails": [  
                            {  
                                "primaryIndustryClassification": "NDY",  
                                "primaryIndustry": "N18",  
                                "industryWeight": 1  
                            }  
                        ]  
                    },  
                    "pd": 0.001788,  
                    "impliedRating":"A3",  
                    "quantitativeInputs": {  
                        "totalAssets": 512.649,  
                        "netSales": 828.886,  
                        "scope1Emission": 77774.65,  
                        "scope2Emission": 15475.56  
                    },
                    "physicalRiskScore": {
                        "physicalRiskScoreOverwrite": 36
                    }  
                }  
            ]  
        }    
    '''
    data=json.loads(json_string)
    json_data = json.dumps(data, indent=4, ensure_ascii=False)
    rs_auth, dict_token = getAuth()
    rs_data, rs_json = getResponse(dict_token, info_type, json_data)
    processId = rs_json.json()["processId"]    
    rs, dl = getProcessResult(dict_token, processId)
    dl_data = dl.json()
    test = json.dumps(dl_data, indent=4)
    #transform json to dataframe
    def jsonToOwnFirmformat(dl_data):
        rows = []
        for entity in dl_data['entities']:
            id_info = [entity['entityId'],entity['asOfDate'],entity['isfin'], entity['physicalRiskScore']]
            riskTypes = ['physicalRisk','transitionRisk','combinedRisk']
            for risktype in riskTypes:
                for keys, values in entity[risktype].items():
                    pd_values = [values['pd'].get(f"pd{i}y", "") for i in range(1, 31)]
                    implied_rating_values = [values['impliedRating'].get(f"impliedRating{i}y", "") for i in range(1, 11)]
                    # Append the row to the list
                    row = id_info + [risktype, keys] + pd_values + implied_rating_values
                    rows.append(row)
            
            pd_values = [entity['baseline']['pd'].get(f"pd{i}y", "") for i in range(1, 31)]
            implied_rating_values = [entity['baseline']['impliedRating'].get(f"impliedRating{i}y", "") for i in range(1, 11)]
            row = id_info + ["none", "baseline"] + pd_values + implied_rating_values
            rows.append(row)
                    
        # Create a DataFrame
        columns = ["entityId", "asOfDate", "isfin", "physicalRiskScore", "riskType", "Scenario"] + \
                  [f"pd{i}y" for i in range(1, 31)] + [f"impliedRating{i}y" for i in range(1, 11)]
    
        df = pd.DataFrame(rows, columns=columns)
        return df

    df_test = jsonToOwnFirmformat(dl_data)

    # test case 7a 
    # ----------------------------------------------------------------
    info_type="reports"
    json_string1 = '''
        {
            "reportType": "climate",
            "reportFormat": "pdf",
            "scenarioCategory": ["NGFS3"],
            "entities": [
                {
                    "entityId": "CN46262PC"
                }
            ]
        }
    '''
    data=json.loads(json_string1)
    json_data = json.dumps(data, indent=4, ensure_ascii=False)
    rs_auth, dict_token = getAuth()
    rs, rs_dl = getResponse(dict_token, info_type, json_data)
    
    for reporturl in rs_dl.json()["reportUrls"]:
        rs1, dl_file =  getDownloadLink(reporturl)
        if rs1 == 200:
            filename = reporturl.split("?response")[0].split("/")[-1]
            fullpath = f"/Users/philipyam/Codings/pythoncodes/moodys_climate_api/02_data/outputs/temp/{filename}"
            with open(fullpath, 'wb') as file:
                file.write(dl_file.content)    
    
    
    # test case 7b
    # ----------------------------------------------------------------
    info_type="reports"
    json_string2 = '''
    {
        "reportType": "climate",
        "reportFormat": "pdf",
        "scenarioCategory": ["NGFS3"],
        "entities": [         
             {
                "entityId": "JP9250001001451",
                "entityName": "UNIQLO",
                "asOfDate": "2023-06-01",
                "qualitativeInputs": {
                    "regionDetails": [
                        {
                            "primaryCountry": "JPN",
                            "primaryCountryWeight": 1
                        }
                    ],
                    "industriesDetails": [
                        {
                            "primaryIndustryClassification": "NDY",
                            "primaryIndustry": "N18",
                            "industryWeight": 1
                        }
                    ]
                },
                "pd": 0.001788,
                "impliedRating":"A3",
                "quantitativeInputs": {
                    "totalAssets": 512.649,
                    "netSales": 828.886,
                    "scope1Emission": 77774.65,
                    "scope2Emission": 15475.56,
                    "physicalRiskScoreOverwrite" : 36
                }
            }     
        ]
    }
    '''
    data=json.loads(json_string2)
    json_data = json.dumps(data, indent=4, ensure_ascii=False)
    rs_auth, dict_token = getAuth()
    rs, rs_dl = getResponse(dict_token, info_type, json_data)
    
    for reporturl in rs_dl.json()["reportUrls"]:
        rs1, dl_file =  getDownloadLink(reporturl)
        if rs1 == 200:
            filename = reporturl.split("?response")[0].split("/")[-1]
            fullpath = f"/Users/philipyam/Codings/pythoncodes/moodys_climate_api/02_data/outputs/temp/{filename}"
            with open(fullpath, 'wb') as file:
                file.write(dl_file.content)    

    
    # test case 8a
    # ----------------------------------------------------------------
    info_type="ESG"
    json_string='''
    [  
        {  
            "batchResponseIdentifier": "Company1",  
            "periodYear": 2021,  
            "regionClassification": "ISO",  
            "regionCode": "US",  
            "industryClassification": "NACE",  
            "industryCode": "35.11",  
            "employeeCount": 150,  
            "assetTurnover": 403.34,  
            "totalAsset": 509.12,  
            "carbonIntensity": "High"  
        }  
    ]    
    '''
    json_string='''
    [  
        {  
            "batchResponseIdentifier": "Company1",  
            "regionClassification": "ISO",  
            "regionCode": "CA"
        }  
    ]    
    '''
    
    
    data=json.loads(json_string)
    json_data = json.dumps(data, indent=4, ensure_ascii=False)
    rs_auth, dict_token = getAuth()
    rs, rs_data = getResponse(dict_token, info_type, json_data)
    json_data = rs_data.json()

    str_json = json.dumps(json_data, indent=4, ensure_ascii=False)

    # test case 8b
    # ----------------------------------------------------------------
    info_type="ESG"
    json_string='''
    [  
        {  
            "batchResponseIdentifier": "Company1",  
            "periodYear": 2021,  
            "regionClassification": "ISO",  
            "regionCode": "US",  
            "industryClassification": "NACE",  
            "industryCode": "35.11",  
            "employeeCount": 150,  
            "assetTurnover": 403.34,  
            "totalAsset": 509.12,  
            "carbonIntensity": "High"  
        },  
        {  
            "batchResponseIdentifier": "Company2",  
            "periodYear": 2021,  
            "regionClassification": "ISO",  
            "regionCode": "CA",  
            "industryClassification": "NACE",  
            "industryCode": "07.29",  
            "employeeCount": 50,  
            "assetTurnover": 123.56,  
            "totalAsset": 160.00  
        } 
    
    ]    
    '''
    data=json.loads(json_string)
    json_data = json.dumps(data, indent=4, ensure_ascii=False)
    rs_auth, dict_token = getAuth()
    rs, rs_data = getResponse(dict_token, info_type, json_data)
    json_data = rs_data.json()
    
    
