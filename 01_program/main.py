# -*- coding: utf-8 -*-
"""
@author:    Philip Yam
@Purpose:   This is a main program to generate moodys climate PD
@Date:      Sep 14 21:00:00 2023
@Remark:    press F9 to run selection    
"""

# =======================================================================================================================
# Set system libraries
# =======================================================================================================================
import pathlib as pl
import datetime
import sys 
import os

# =======================================================================================================================
# Initialize paths
# =======================================================================================================================

dict_paths = {
    'root_path':         pl.Path(__file__).parent.absolute(),  # Gets the directory of the script
    'folder_pgm':        "/01_program",
    'folder_pgmmodu':    "/modules",
    'folder_intray':     "/02_in_tray",
    'folder_outtray':    "/03_out_tray",
    'folder_template':   "/template", 
    'folder_pickle':     "/pickles",
    'output_climatePD':  "/deliverable_climate_pds",
    'output_transrisk':  "/deliverable_transition_risk_drivers",
    'output_reports':    "/deliverable_predefined_reports",
    'output_esg':    "/deliverable_esg",
}

print('Absolute path : {}'.format(pl.Path().absolute()))
print(f"Current path : {pl.Path.cwd()}")
print(f"Home path : {pl.Path.home()}")
os.chdir(dict_paths['root_path']+dict_paths['folder_pgm']) 
print(f"New current directory : {pl.Path.cwd()}")
sys.path.append(os.path.abspath(dict_paths['root_path']+dict_paths['folder_pgm']+dict_paths['folder_pgmmodu']))

# =======================================================================================================================
# Main procedures
# =======================================================================================================================
import file_handlers as fh
import ownfirm_data_formatters as adf
import ownfirm_models as amodel
import ownfirm_to_moodys_connectors as amc


if __name__ == '__main__':
    dtts_begin = datetime.datetime.now()
    path_intray = dict_paths['root_path']+dict_paths['folder_intray']
    path_outray = dict_paths['root_path']+dict_paths['folder_outtray']
    
    dict_xlsxmeta, dict_xlsx = fh.readXLSX(path_intray) #read excel files
    path_target, name_target = fh.createFolder(path_outray, dict_xlsxmeta['name'], dtts_begin)
    path_pickle = fh.createFolder(path_target+dict_paths['folder_pickle'])

    # create log file    
    logger = fh.createLog(path_target, name_target)

    # obtain Input Table and Parameters in dataframe format
    df_inputtable = adf.getInputTable(dict_xlsx)
    df_cpdproperties = adf.getCPDProperties(dict_xlsx)
    dict_dcontrol = adf.getDeliverableControl(dict_xlsx)

    if dict_dcontrol['Retrieve Climate Adjusted PDs'] == 'ENABLE':
        path_climatePD = fh.createFolder(path_target+dict_paths['output_climatePD'])
        bool_isasync, list_apiinputs_climatepds = adf.genListOfAPIInput_ClimatePDs(df_cpdproperties, df_inputtable, logger)    
        list_responses_climatepds = amc.obtainClimatePDs(bool_isasync, list_apiinputs_climatepds, logger)
        fh.writeBinary(path_pickle, list_responses_climatepds, "list_responses_climatepds")
        list_ownfirmoutputs_climatepds = adf.extractAPIOutput_ClimatePDs(list_responses_climatepds, logger)
        adf.exportAPIOutput_ClimatePDs(path_climatePD, list_ownfirmoutputs_climatepds, logger)

        df_portfolio_edf = amodel.calculatePortfolioPD (list_ownfirmoutputs_climatepds)
        adf.exportPortfolioPDs(path_climatePD, df_portfolio_edf, logger)
    
    
    if dict_dcontrol['Retrieve Transition Risk Drivers for Industry (Sector)'] == 'ENABLE':
        path_transrisk = fh.createFolder(path_target+dict_paths['output_transrisk'])
        dict_apiinputs_industry = adf.genAPIInput_TransRiskIndustry(df_cpdproperties, df_inputtable, logger)
        obj_responses_industry = amc.obtainTransRiskIndustry(dict_apiinputs_industry, logger)
        fh.writeBinary(path_pickle, obj_responses_industry, "obj_responses_industry")
        df_ownfirmoutputs_industry = adf.extractAPIOutput_TransRiskIndustry(obj_responses_industry, logger)
        adf.exportAPIOutput_TransRiskIndustry(path_transrisk, df_ownfirmoutputs_industry, logger)
 
        
    if dict_dcontrol['Retrieve Transition Risk Drivers for Country (Region)'] == 'ENABLE':
        path_transrisk = fh.createFolder(path_target+dict_paths['output_transrisk'])
        dict_apiinputs_region = adf.genAPIInput_TransRiskRegion(df_cpdproperties, df_inputtable, logger)
        obj_responses_region = amc.obtainTransRiskRegion(dict_apiinputs_region, logger)
        fh.writeBinary(path_pickle, obj_responses_region, "obj_responses_region")
        df_ownfirmoutputs_region = adf.extractAPIOutput_TransRiskRegion(obj_responses_region, logger)
        adf.exportAPIOutput_TransRiskRegion(path_transrisk, df_ownfirmoutputs_region, logger)
    
    
    if dict_dcontrol['Access Pre-defined reports'] == 'ENABLE':
        # only available when entity Id is valid BVD id
        path_reports = fh.createFolder(path_target+dict_paths['output_reports'])
        list_apiinputs_reports = adf.genListOfAPIInput_Reports(df_cpdproperties, df_inputtable, logger)    
        # as response has expiring time limit, it must be handled one by one, not in batch mode
        amc.downloadReports(path_reports, list_apiinputs_reports, logger)
    
    
    if dict_dcontrol['Request ESG Score Predictor'] == 'ENABLE':
        path_esg = fh.createFolder(path_target+dict_paths['output_esg'])
        
        list_apiinputs_esg = adf.genAPIInput_ESG(df_cpdproperties, df_inputtable, logger)    
        response_esg = amc.obtainESG(list_apiinputs_esg , logger)
        fh.writeBinary(path_pickle, response_esg, "response_esg")
        df_ownfirmoutputs_esg = adf.extractAPIOutput_ESG(response_esg, logger)
        adf.exportAPIOutput_ESG(path_esg, df_ownfirmoutputs_esg, logger)    
        
    """
    # debug
    list_loadback = fh.readBinary(path_pickle, "list_responses_climatepds")
    """
    dtts_finish = datetime.datetime.now()
    fh.closeLog(logger, dtts_begin, dtts_finish) 
    
    fh.moveFiles(dict_xlsxmeta['file'],path_intray,path_target)

