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
import shutil
import re
import yaml
import requests
import click

from pathlib import Path
from datetime import date
from jsonschema import ValidationError
from heal.vlmd import vlmd_extract, ExtractionError

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

def create_directory_structure(output_path: Path, hdp_id: str, project_name:str = None):
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


def process_files(
    clean_study_path: Path,
    output_study_path: Path,
    appl_id: str,
    hdp_id: str,
    project_title: str,
    project_type: str,
    overwrite: bool = False
):
    """
    Process CSV files by extracting VLMD (JSON) using `vlmd_extract`.  
    If extraction fails (schema or other), log and skip.
    On success, copy the original CSV to `input/` and generate metadata.yaml.

    Parameters:
        clean_study_path (Path): directory with cleaned data dictionaries (CSVs).
        output_study_path (Path): base output directory for this study.
        appl_id (str): Application identifier.
        hdp_id (str): HEAL Data Platform identifier.
        project_title (str): Title of the project.
        project_type (str): Type of the project (e.g., "clinical", "lab", etc.).
        overwrite (bool): If True, always re‐run extraction (even if JSON exists).
    """
    print(f"Looking for CSVs under: {clean_study_path}")
    file_list = [f for f in clean_study_path.glob("*.csv") if not f.name.startswith(".")]
    logging.info(f"Found {len(file_list)} CSV file(s) in {clean_study_path}")
    logging.debug(file_list)

    valid_count = 0

    for file_path in file_list:
        # We assume detect_input_type only controls logging/skipping; vlmd_extract auto‐detects format.
        input_type = detect_input_type(str(file_path))
        if input_type is None:
            logging.info(f"Skipping (non‐compliant) file: {file_path.name}")
            continue

        logging.info(f">>> Processing file: {file_path.name}  (detected input_type = {input_type})")
        dd_folder_name = file_path.stem.replace(" ", "_")
        vlmd_subdir = f"vlmd/{dd_folder_name}"
        output_dd_folder_path = output_study_path / vlmd_subdir
        output_dd_folder_path.mkdir(parents=True, exist_ok=True)

        metadata_yaml_path = output_dd_folder_path / "metadata.yaml"
        overwrite_if_no_yaml = overwrite or not metadata_yaml_path.exists()

        # Before calling vlmd_extract: if the JSON already exists and overwrite is False, skip.
        # 1) Pre-check for an existing heal-dd_<stem>.json
        emitted_name = f"heal-dd_{dd_folder_name}.json"
        emitted_path = output_dd_folder_path / emitted_name

        if emitted_path.exists() and not overwrite_if_no_yaml:
            final_json_path = emitted_path
            logging.info(
                f"Skipping extraction; found existing {emitted_name} and overwrite=False"
            )
            valid_count += 1

        else:
            # 2) Run the new extractor
            try:
                vlmd_extract(
                    str(file_path),
                    title=dd_folder_name,
                    output_dir=str(output_dd_folder_path)
                )

                # 3) Verify it actually wrote heal-dd_<stem>.json
                if not emitted_path.exists():
                    raise FileNotFoundError(
                        f"Expected output {emitted_name} in {output_dd_folder_path}"
                    )

                # 4) Rename to prepend your HDP ID, if needed
                prefix = f"{hdp_id}_"
                target_name = (
                    f"{prefix}{dd_folder_name}.json"
                    if not emitted_path.name.startswith(prefix)
                    else emitted_path.name
                )
                final_json_path = output_dd_folder_path / target_name
                if emitted_path.name != target_name:
                    emitted_path.rename(final_json_path)

            except ValidationError as v_err:
                logging.error(f"[ValidationError] {file_path.name} → {v_err}")
                continue
            except ExtractionError as e_err:
                logging.error(f"[ExtractionError] {file_path.name} → {e_err}")
                continue
            except FileNotFoundError as fnf:
                logging.error(f"[FileNotFoundError] {fnf}")
                continue
            else:
                valid_count += 1

        # At this point, the JSON exists at final_json_path. Proceed to copy original CSV.
        input_dest_dir = output_study_path / "input"
        input_dest_dir.mkdir(parents=True, exist_ok=True)

        input_dest = input_dest_dir / file_path.name
        shutil.copyfile(file_path, input_dest)

        # Build file_config (pointing at the .json under vlmd/, rather than .vlmd.csv)
        # Note: We replace local paths with GitHub URLs using regex substitutions.
        file_config = {
            "inputtype": input_type,
            "input_filepath": re.sub(
                r".*input/",
                f"https://github.com/heal-data-stewards/heal-data-dictionaries/tree/main/data-dictionaries/{output_study_path.name}/input/",
                str(input_dest)
            ),
            "output_filepath": re.sub(
                r".*vlmd/",
                f"https://github.com/heal-data-stewards/heal-data-dictionaries/tree/main/data-dictionaries/{output_study_path.name}/vlmd/",
                str(final_json_path)
            ),
            "relative_input_filepath": re.sub(r".*input/", "../input/", str(input_dest)),
            "relative_output_filepath": re.sub(r".*vlmd/", "../", str(final_json_path))
        }

        # Write metadata.yaml (using the same helper as before).
        file_configs = {dd_folder_name: file_config}
        create_metadata_yaml(
            metadata_yaml_path,
            hdp_id,
            appl_id,
            project_title,
            file_configs,
            dd_folder_name,
            project_type
        )

        logging.info(f"Successfully processed {file_path.name}; VLMD JSON at {final_json_path.name}")

    logging.info(
        f"Found {len(file_list)} file(s) in {clean_study_path}. "
        f"Valid VLMD conversions: {valid_count}"
    )


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
    
    output_study_path = create_directory_structure(output_path=output_path, hdp_id=hdp_id, project_name=project)

    logging.info("Converting files using HEAL Data Utils tool")
    # Process each file and collect configuration details
    process_files(clean_study_path=clean_study_path, output_study_path=output_study_path, hdp_id=hdp_id, appl_id=appl_id, project_title=project_title, project_type=project_type, overwrite=overwrite)
    logging.info(f"DONE processing files in {clean_study_path}")

if __name__ == "__main__":
    process_study_files()
