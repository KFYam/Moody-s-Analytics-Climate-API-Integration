# Moody's Analytics Climate API Integration

## Overview
This repository provides a Python-based solution to interact with Moody's Analytics Climate API (EDF-X and ESG). The program retrieves Probability of Default (PD) and other climate-adjusted metrics based on input data from an Excel file, and saves the results to an output directory.

## Features
- **Input Management**: Reads input parameters and entity details from an Excel template.
- **Climate PD Retrieval**: Fetches climate-adjusted PDs using Moody's Climate API.
- **Transition Risk Drivers**: Retrieves industry and regional transition risk details.
- **Pre-defined Reports**: Downloads detailed climate and ESG reports (if enabled).
- **Portfolio Analysis**: Calculates portfolio-level PD metrics.

## Repository Structure
project_root/
├── 01_program/                            # Main program folder 
│   ├── main.py                            # Entry point for the application 
│   ├── modules/                           # Custom modules 
│   ├── file_handlers.py 
│   ├── moodys_climate_api.py 
│   ├── ownfirm_data_formatters.py 
│   ├── ownfirm_models.py 
│   ├── ownfirm_to_moodys_connectors.py 
├── 02_in_tray/                            # Folder for input files 
│   ├── template/                          # Input template files 
│   └── Input Template.xlsx 
├── 03_out_tray/                           # Folder for generated output 
├── config.json                            # Configuration file for API credentials 
└── README.md # Project documentation

## Getting Started

### Prerequisites
- Python 3.8 or higher
- An active account with Moody's Analytics Climate API access

### Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/your-repository.git
   cd your-repository

2. Install dependencies:
   ```bash
   pip install -r requirements.txt

3. Configure the API credentials:
Update the config.json file with your Moody's API credentials.
   ```json
   {
       "auth": {
           "clientId": "your_client_id",
           "clientSecret": "your_client_secret",
           "URL": "https://sso.moodysanalytics.com/sso-api/v1/token"
       }
   }
