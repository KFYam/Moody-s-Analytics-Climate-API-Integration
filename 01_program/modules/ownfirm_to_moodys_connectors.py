import moodys_climate_api as mapi
import json
import time

# %%
def obtainClimatePDs(bool_isasync, list_apiinputs_climatepds, logger):
    info_type = "climate_pds_TPC"
    list_responses_climatepds = []
    count_loop = 0
    
    logger.info("Begin to request climate-adjusted PDs ...") if logger is not None else None 
    
    for apiinput in list_apiinputs_climatepds:
        count_loop += 1
        
        json_str_input = json.dumps(apiinput, indent=4, ensure_ascii=False) 
        returncode_auth, dict_token = mapi.getAuth()
        
        if bool_isasync:
            logger.info(f"-> Execute iteration # {count_loop} of Total # {len(list_apiinputs_climatepds)}") if logger is not None else None 
            logger.info("--> Get a processID for this request ...") if logger is not None else None 
            returncode_pid, response_pid = mapi.getResponse(dict_token, info_type, json_str_input)
            str_processId = response_pid.json()["processId"] 
            
            logger.info("--> Download climate-adjusted PDs by this processID ...") if logger is not None else None 
            returncode_pds, response_pds = mapi.getProcessResult(dict_token, str_processId)
            list_responses_climatepds.append(response_pds)
            
        else:
            logger.info(f"-> Get climate-adjusted PDs at iteration #{count_loop} of Total #{len(list_apiinputs_climatepds)}") if logger is not None else None 
            returncode_pds, response_pds = mapi.getResponse(dict_token, info_type, json_str_input)
            list_responses_climatepds.append(response_pds)
            time.sleep(2) # let time to append the response object into memory
            
    logger.info("Finish requesting climate-adjusted PDs") if logger is not None else None 
    
    return list_responses_climatepds


# %%
def obtainTransRiskIndustry(dict_apiinputs_industry, logger):
    info_type="industry_T"
    
    logger.info("Begin to request Transition Risk Drivers for Industry ...") if logger is not None else None 
    
    returncode_auth, dict_token = mapi.getAuth()
    returncode_industry, response_industry = mapi.getResponse(dict_token, info_type, dict_apiinputs_industry)
  
    response_dl = None
    if returncode_industry == 200: 
        str_downloadlink = response_industry.json()["downloadLink"]
        returncode_dl, response_dl =  mapi.getDownloadLink(str_downloadlink)
        
        logger.info("Finish requesting Transition Risk Drivers for Industry") if logger is not None else None 
    else:
        
        logger.info("Error when requesting Transition Risk Drivers for Industry") if logger is not None else None 
            
    return response_dl


# %%
def obtainTransRiskRegion(dict_apiinputs_region, logger):
    info_type="region_T"
    
    logger.info("Begin to request Transition Risk Drivers for Region ...") if logger is not None else None 
    
    returncode_auth, dict_token = mapi.getAuth()
    returncode_region, response_region = mapi.getResponse(dict_token, info_type, dict_apiinputs_region)
  
    response_dl = None
    if returncode_region == 200: 
        str_downloadlink = response_region.json()["downloadLink"]
        returncode_dl, response_dl =  mapi.getDownloadLink(str_downloadlink)
        
        logger.info("Finish requesting Transition Risk Drivers for Region") if logger is not None else None 
    else:
        
        logger.info("Error when requesting Transition Risk Drivers for Region") if logger is not None else None 
            
    return response_dl


#%%
def obtainESG(list_apiinputs_esg , logger):
    info_type="ESG"
    
    logger.info("Begin to request ESG Score Predictor ...") if logger is not None else None 
    
    json_str_input = json.dumps(list_apiinputs_esg , indent=4, ensure_ascii=False) 
    returncode_auth, dict_token = mapi.getAuth()
    returncode_esg, response_esg = mapi.getResponse(dict_token, info_type, json_str_input)
    
    
    return response_esg 

# %%
def downloadReports(path_reports, list_apiinputs_reports, logger):
    info_type="reports"
    
    logger.info("Begin to access Pre-defined Reports ...") if logger is not None else None 
    
    count_loop = 0    
    for dict_apiinputs_report in list_apiinputs_reports:
        count_loop +=1
        
        json_str_input = json.dumps(dict_apiinputs_report, indent=4, ensure_ascii=False) 
        returncode_auth, dict_token = mapi.getAuth()
        returncode_report, response_report = mapi.getResponse(dict_token, info_type, json_str_input)
  
        if returncode_report == 200: 
            for reporturl in response_report.json()["reportUrls"]:
                returncode_file_dl, file_dl =  mapi.getDownloadLink(reporturl)
                if returncode_file_dl == 200:
                    filename = reporturl.split("?response")[0].split("/")[-1]
                    fullpath = f"{path_reports}/{filename}"
                    with open(fullpath, 'wb') as file:
                        file.write(file_dl.content)            
            logger.info(f"-> CSV,PDF are downloaded at iteration # {count_loop} of Total # {len(list_apiinputs_reports)}") if logger is not None else None 
        elif "detail" in response_report.json():
            logger.info(f"->Error message :{response_report.json()['detail']} at iteration # {count_loop} of Total # {len(list_apiinputs_reports)}") if logger is not None else None 
        elif "errorMessage" in response_report.json():
            logger.info(f"->Error message :{response_report.json()['errorMessage']} at iteration # {count_loop} of Total # {len(list_apiinputs_reports)}") if logger is not None else None 
        else:
            logger.info(f"->Error message found which return code is {returncode_report} at iteration # {count_loop} of Total # {len(list_apiinputs_reports)}") if logger is not None else None 

    logger.info("Finished accessing Pre-defined Reports") if logger is not None else None 
            
    return None


