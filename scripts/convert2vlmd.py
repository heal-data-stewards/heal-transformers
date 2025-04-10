#!/usr/bin/env python
"""
HEAL Data Dictionaries Converter

This script processes CSV/Stata files containing representing data dictionaries by converting them
to VLMD format and generating corresponding metadata YAML files. 
The script retrieves project metadata from the HEAL data service, 
creates the required directory structure, and processes each data dictionary file.

The following command-line arguments are supported:

  --clean_dd_directory: Directory containing input data dictionaries (default: DataDictionaries/AssignedDataDictionaries/HDPXXXX)
  --output_directory: Directory to store output files. Outputs will be stored in data-dictionaries subfolder of this directory. (default: DataDictionaries/CleanedDataDictionaries)
  --hdp_id: HEAL project ID; defaults to the project identifier if not provided
  --appl_id: APPL ID (project award nunber), defaults to None. This is optional to provide.
  --project_type: Study type (eg.HEAL Research Programs, HEAL  Research Networks, HEAL Study), (default="Research Programs")
  --overwrite: Option to overwrite existing VLMDs instead of asking to delete manually (default=False)

Example usage:
    ./scripts/convert2vlmd.py --clean_dd_directory input --output_directory output --hdp_id HDP12345--overwrite True --project_type "HEAL Study"
"""
import logging
import os
import pandas as pd
from pathlib import Path
import shutil
import re
import yaml
import requests
from datetime import date
import click

from healdata_utils.conversion import convert_to_vlmd

# Configure logging: adjust level and format as needed
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(message)s")

# print(json.dumps(input_short_descriptions, indent=2))
pd.set_option("future.no_silent_downcasting", True)

def create_metadata_yaml(yaml_path: Path, hdp_id: str, appl_id: str, project_title: str, file_configs: dict, file_name: str, project_type: str = "HEAL Research Programs"):
    """
    Create a metadata YAML file with project details and file-specific configurations.

    Parameters:
        yaml_path (Path): Path where the YAML file will be saved.
        hdp_id (str): HDP identifier.
        appl_id (str): Application identifier.
        project_title (str): Title of the project.
        file_configs (dict): Dictionary containing configuration info for each file.
    """
    # Define the base project metadata
    config = {
        'Project': {
            'HDP_ID': hdp_id,
            'Filename': file_name,
            'ProjectType': project_type,
            'LastModified': str(date.today()),
            'ProjectTitle': project_title,
            'Status': 'Draft'
        }
    }
    if appl_id:
        config['Project']['APPL_ID'] = appl_id
    # Merge file-specific configurations into the overall config
    config.update(file_configs)

    # Write the configuration dictionary to the YAML file
    with open(yaml_path, 'w') as f:
        yaml.dump(config, f)
    logging.info(f"Metadata YAML created at: {yaml_path}")


def get_base_path():
    """Returns the base path by stripping the trailing directories."""
    return os.path.dirname(os.path.dirname(os.getcwd()))


def detect_input_type(filepath: str):
    """Detects the appropriate input type based on file extension."""
    if filepath.endswith(".dta"):
        input_type= "stata"
    elif filepath.endswith(".data-dict.csv"):
        input_type="csv-data-dict"
    elif filepath.endswith(".redcap.csv"):
        input_type="redcap-csv"
    elif filepath.endswith('.csv'):
        input_type = "redcap-csv"
    else:
        input_type = None
    return input_type 

def create_directory_structure(output_path: Path, clean_study_path: Path, hdp_id: str, project_name:bool=None):
    """
    Create required directories for the application and temporary working space.

    In the path provided under output_path, this function creates
    - An HDP subdirectory (named by hdp_id) under the application directory.
    - Two subdirectories under the HDP directory: 'vlmd' for converted files and 'input' for original files.
    
    Parameters:
        output_path (Path): Path to the clone of heal-data-dictionaries folder
        clean_study_path (Path) : Path to the directory that holds cleaned data dictionaries for this study.
        appl_id (str): Application identifier.
        hdp_id (str): HDP identifier.

    Returns:
        tuple: hdp_dir as Path objects.
    """
    # Create the application directory if it doesn't exist
    parent_path = output_path / f"data-dictionaries/{hdp_id}" if project_name is None else output_path / f"data-dictionaries/{project_name}"
    input_dir =  parent_path / "input"
    vlmd_dir = parent_path / "vlmd"

    input_dir.mkdir(parents=True, exist_ok=True)
    vlmd_dir.mkdir(parents=True, exist_ok=True)

    return parent_path


def search_for_key(data, target_key):
    """
    Recursively search for the first occurrence of a key in a nested data structure.

    Parameters:
        data: A dictionary or list to search.
        target_key (str): The key to search for.

    Returns:
        The value associated with target_key if found; otherwise, None.
    """
    if isinstance(data, dict):
        for key, value in data.items():
            if key == target_key:
                return value
            result = search_for_key(value, target_key)
            if result is not None:
                return result
    elif isinstance(data, list):
        for item in data:
            result = search_for_key(item, target_key)
            if result is not None:
                return result
    return None

def query_mds(query_params:dict):
    """
    Retrieve metadata JSON from a URL based on information requested.

    This function makes a request to:
        https://healdata.org/mds/metadata/
    
    Parameters:
        query_params (dict): Can either have {'hdp_id': <hdp_id>} OR
                                             {'appl_id': <appl_id>}

    Returns:
        data: JSON response if found else None.
    """
    if 'hdp_id' in query_params:
        url = f"https://healdata.org/mds/metadata/{query_params['hdp_id']}"
    elif 'appl_id' in query_params:
        url = f"https://healdata.org/mds/metadata?data=True&offset=0&nih_reporter.appl_id={query_params['appl_id']}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error retrieving appl_id: {e}")
        data = None

    return data

def determine_appl_id(hdp_id:str):
    """
    Retrieve metadata JSON from a URL and search for any occurrence of the key 'appl_id'.

    This function makes a request to the MDS, and
        recursively searches the returned JSON for 'appl_id'.

    Parameters:
        hdp_id (str): The HDP identifier.

    Returns:
        str: The application ID if found; otherwise, an empty string.
    """
    mds_response = query_mds({'hdp_id':hdp_id})
    if mds_response is None:
        return ""
    # Should be careful wit this construction. Especially if there are multiple APPLIDs associated with one HDPID.
    found_appl_id = search_for_key(mds_response, "appl_id")
    if found_appl_id:
        logging.info(f"Found appl_id: {found_appl_id}")
        return found_appl_id
    else:
        logging.warning("appl_id not found in the JSON response.")
        return None

def determine_hdp_id(appl_id:str):
    """
    Retrieve metadata JSON from a URL and search for 'hdp_id' given an 'appl_id'

    This function makes a request to the MDS, and
        searches the returned JSON for 'hdp_id'.

    Parameters:
        appl_id (str): The APPLID of the award.

    Returns:
        str: The study HDPID if found; otherwise, an empty string.
    """

    mds_response = query_mds({'appl_id':appl_id})
    logging.info(f"Total keys in response: {len(mds_response)}")
    first_key = list(mds_response.keys())[0]
    hdp_id = mds_response[first_key]['gen3_discovery'].get('_hdp_uid', None)
    return hdp_id

def fetch_project_metadata(hdp_id: str):
    """
    Fetch metadata from the HEAL data service for a given application and HDP identifier.

    Parameters:
        hdp_id (str): HDP identifier.

    Returns:
        tuple: (project_title, updated_hdp_id)
    """
    
    ## Use HDPID to query the MDS in order to get the project title
    mds_response = query_mds({'hdp_id':hdp_id})
    if mds_response is None:
        ## If HDPID not found in MDS, query by appl_id, and try to get thhe HDPID that way
        logging.error("Given HDPID was not found in MDS. Check the HDPID, and rerun")
        return None
    project_title = "NOT FOUND"
    project_title = mds_response['nih_reporter'].get('project_title', project_title) if 'nih_reporter' in mds_response else 'NOT FOUND'
    
    return project_title

def process_files(clean_study_path:Path, output_study_path: Path, appl_id: str, hdp_id:str, project_title: str, project_type: str, overwrite:bool = False):
    """
    Process CSV files by converting them to VLMD format using multiple input types.
    If conversion fails for all supported types, log an error message indicating that the file requires additional cleaning.

    Parameters:
        clean_study_path (Path): path to the directory with cleaned data dictionaries
        file_list (list): List of CSV file paths.
        hdp_dir (Path): Final HDP directory.
        appl_id (str): Application identifier.
        raw_name (str): Raw study name.
        project_title (str): Title of the project.

    Returns:
        dict: file_configs with file names as keys and configuration dictionaries as values.
    """

    ### Create a list of files fromt the clean study directory
    ### Process each file in this list -> convert, create file_config, create metadata_yaml, and create corresponding directory
    print(clean_study_path)
    file_list = [f for f in clean_study_path.glob("*") if not f.name.startswith('.')]
    print(file_list)
    logging.info(f"Found {len(file_list)} files for study in dir {clean_study_path}")
    logging.debug(file_list)


    valid_count = 0
    for file_path in file_list:
        input_type = detect_input_type(str(file_path))
        if input_type is None:
            logging.info(f"Skipping non-compliant file: {file_path}")
            continue

        data_dictionaries = None

        ## Inside vlmd folder, create a directory for this file. 
        # Process, and create output in this subfolder
        # Create file config, and create metadata.yaml file for this file.

        # Try multiple input types: first "csv-data", then "redcap-csv"
        logging.info(f'>>> Processing file: {file_path} with input type: {input_type}')
        dd_folder_name = file_path.stem.replace(' ', '_')
        vlmd_subpath = f"vlmd/{dd_folder_name}/" # Allowing this subfolder generation even if one file is available to account for the case when there are more than one files being processed one after the other.
        output_dd_folder_path = output_study_path / vlmd_subpath
        output_dd_folder_path.mkdir(parents=True, exist_ok=True)

        metadata_yaml_path = output_dd_folder_path / "metadata.yaml"
        ## If metadata yaml file does not exist, chances are that the generated vlmd files (if they exist) were generated in error.
        ## Overwrite any existing vlmd files when the metadata.yaml file does not exist.
        overwrite_if_no_yaml = overwrite or not metadata_yaml_path.exists()

        output_file = output_dd_folder_path / f"{hdp_id}_{dd_folder_name}.vlmd.csv"
        description = f"DD converted using healdata-utils for input type {input_type}"
        try:
            data_dictionaries = convert_to_vlmd(
                input_filepath=str(file_path),
                output_filepath=str(output_file),
                inputtype=input_type,
                data_dictionary_props={"title": dd_folder_name, "description": description},
                output_overwrite=overwrite_if_no_yaml
            )
        except FileExistsError:
            logging.info(f'{dd_folder_name} : {description} already processed')
            continue
        else:
            # Check if conversion was valid
            if not (data_dictionaries['errors']['csvtemplate']['valid'] and data_dictionaries['errors']['jsontemplate']['valid']):
                logging.error(f"File {file_path} requires additional cleaning before processing. Not creating Metadata.yaml")
                continue
            
            valid_count +=1
            ## If the conversion was successful, create the config file, and copy the input file to the input directory
            # Copy the original CSV file to the "input" directory with a new name
            input_dest = output_study_path / f"input/{file_path.name}"
            shutil.copyfile(file_path, input_dest)
            
            vlmd_json_dest = str(output_file).replace(".csv", ".json")
            # Prepare file configuration using regex substitutions to create GitHub URL paths
            file_config = {
                'inputtype': input_type,
                'input_filepath': re.sub(".*input/",
                                        f"https://github.com/heal-data-stewards/heal-data-dictionaries/tree/main/data-dictionaries/{output_study_path.name}/input/",
                                        str(input_dest)),
                'output_filepath': re.sub(".*vlmd/",
                                        f"https://github.com/heal-data-stewards/heal-data-dictionaries/tree/main/data-dictionaries/{output_study_path.name}/vlmd/",
                                        str(vlmd_json_dest)),
                'relative_input_filepath': re.sub(".*input/", "../input/", str(input_dest)),
                'relative_output_filepath': re.sub(".*vlmd/", "../", str(vlmd_json_dest))
            }
            file_configs = {dd_folder_name: file_config}
            create_metadata_yaml(metadata_yaml_path, hdp_id, appl_id, project_title, file_configs, dd_folder_name, project_type)

            # Final logging statement indicating successful processing and readiness for GitHub upload
            logging.info("CSV, VLMD, and metadata YAML files have been created and are ready for upload to GitHub.")

    logging.info(f"Found {len(file_list)} files in {clean_study_path}. Conversion tool was able to create valid vlmd files for {valid_count} files")

# Set up command line arguments.
@click.command()
@click.option(
    "--clean_study_directory",
    type=str, 
    help="Path to a directory with cleaned data dictionaries to be processed.\nAll csv files within this directory will be processed, converted and put into a directory structure under output_dir path"
)
@click.option(
        "--output_directory",
        type=str, 
        help="This is path to idally the clone of heal-data-dictionaries repository, but can be any folder.\nA folder with the name of clean_dd_dir will be created under <output_dir>/data-dictionaries folder, and necessary structure created underneath"
)
@click.option(
    "--hdp_id", 
    required=True, 
    type=str, 
    help="HDPID for this study (eg. HDP00223)"
)
@click.option(
    "--appl_id",
    type=str,
    help="APPLID is the award number associated with this study. Optional argument",
    default=None
)
@click.option(
    "--project",
    type=str,
    help="Directory name to be used instead of provided hdp_id to write output files to",
    default=None
)
@click.option(
    "--project_type", 
    type=str, 
    help="Study type (eg.Research Programs, Research Networks, etc)", 
    default="Research Programs"
)
@click.option(
    "--overwrite",
    is_flag = True,
    help="Option to overwrite existing VLMDs instead of asking to delete manually",
    default=False
)
def process_study_files(clean_study_directory:str, output_directory:str, hdp_id:str, appl_id:str, project:str, project_type:str, overwrite: bool):

    # Fetch project metadata from the HEAL data service
    logging.info("Getting Project Metadata from MDS")
    project_title = fetch_project_metadata(hdp_id)
    if project_title is None:
        return
    
    ## If project title was not found, ask user if they would like to provide a project title.
    if project_title=='NOT FOUND':
        project_title = input("Querying MDS did not get the project title. Enter the project title: ")

    if appl_id is None:
        appl_id = determine_appl_id(hdp_id=hdp_id)

    logging.info("Creating output directory structure")
    # Set up required directory structure for outputs and temporary work
    clean_study_path = Path(clean_study_directory)
    if not clean_study_path.exists():
        logging.error("Invalid Input Directory Path. Please provide a valid path")
        return
    # Path to clone of Platform's heal-data-dictionaries directory
    output_path = Path(output_directory)
    if not output_path.exists():
        logging.error("Invalid Output Path. Please provide a valid path to the clone of heal-data-dictionaries github repository")
        return
    
    output_study_path = create_directory_structure(output_path=output_path, clean_study_path=clean_study_path, hdp_id=hdp_id, project=project)

    logging.info("Converting files using HEAL Data Utils tool")
    # Process each file and collect configuration details
    process_files(clean_study_path=clean_study_path, output_study_path=output_study_path, hdp_id=hdp_id, appl_id=appl_id, project_title=project_title, project_type=project_type, overwrite=overwrite)
    logging.info(f"DONE processing files in {clean_study_path}")

if __name__ == "__main__":
    process_study_files()
