import os
import pandas as pd
import warnings
import pickle
import logging
# pip install openpyxl


# %%
def nan_to_none(x):
    return None if isinstance(x, (int, float)) and pd.isna(x) else x

# %%
def readXLSX(folder_path):
    # Create a dictionary to store dataframes
    dict_sheet = {}
    filename_target = None
    
    # Loop through the files in the folder
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
    
        # Check if the file is an XLSX or CSV file based on the file extension
        if filename.endswith('.xlsx') and filename[:1] != "~":
            warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')
            excel_file = pd.ExcelFile(file_path)
            # Get the list of sheet names in the Excel file
            sheet_names = excel_file.sheet_names
            # Iterate through each sheet and read it into a dataframe
            for sheet_name in sheet_names:
                # Now, you have a dictionary of dataframes where the keys are the sheet names
                # and the values are the corresponding dataframes.
                dict_sheet[sheet_name] = excel_file.parse(sheet_name).apply(nan_to_none).replace(float('nan'), None)
                
            filename_target = filename    
            # Close the Excel file
            excel_file.close()
            
        elif filename.endswith('.csv') and filename[:1] != "~":
            filename_target = filename    
            # Read CSV file into a DataFrame
            dict_sheet[os.path.splitext(filename)[0]] = pd.read_csv(file_path).map(nan_to_none)

    dict_meta  = {"name": os.path.splitext(filename_target)[0], "file": filename_target}
    
    return dict_meta, dict_sheet

# %%
def createFolder(root_path=None, target_folder=None, ts=None):
    if target_folder is not None and ts is not None:
        full_path = root_path+"/"+target_folder+"_"+ts.strftime("%Y%m%d_%H%M%S")
        # Use the os.makedirs() function to create the folder and its parent directories if they don't exist
        os.makedirs(full_path)
        return full_path, target_folder+"_"+ts.strftime("%Y%m%d_%H%M%S")
    
    elif root_path is not None and target_folder is None and ts is None:
        if os.path.exists(root_path):
            None
        else: 
            os.makedirs(root_path)
            
        return root_path
    else:
        return None

# %%
def writeBinary(path, data, name):
    if data is not None:
        with open(f"{path}/{name}.pkl", 'wb') as file:
            return pickle.dump(data, file)    
    else:
        return None

def readBinary(path, name):
    with open(f"{path}/{name}.pkl", 'rb') as file:
        return pickle.load(file)    

# %%
def moveFiles(filename, src_path, dest_path):
    rs = False
    
    for filename in os.listdir(src_path):
        if os.path.isfile(src_path+"/"+filename):
            src_file = os.path.join(src_path, filename)
            dest_file = os.path.join(dest_path, filename)
            os.rename(src_file, dest_file)
            rs = True

    return rs

# %%
def createLog(log_path, log_name):
    # create a log file
    st_log = log_path+"/"+log_name+'.log'
    logging.basicConfig(
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.INFO, 
        force=True, 
        filename=st_log
    )
        
    logger = logging.getLogger() 
    logger.info(f"Process is Started; log file of {log_name} is created")

    return logger

#%%
def closeLog(logger, dtts_begin, dtts_finish):
    dtts_diff = (dtts_finish-dtts_begin).total_seconds()/60
    logger.info(f"Process is Completed, total processing time (in min.): {dtts_diff}")
    
    return logging.shutdown()
