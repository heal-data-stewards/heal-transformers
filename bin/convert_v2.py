#!/usr/bin/env python
"""
HEAL Data Dictionaries Converter

This script processes CSV files containing HEAL data dictionaries by converting them
to VLMD format and generating a metadata YAML file. The script retrieves project metadata
from the HEAL data service, creates the required directory structure, and processes each CSV file.
The following command-line arguments are supported:

  --input_directory: Directory containing input data dictionaries (default: DataDictionaries/AssignedDataDictionaries)
  --output_directory: Directory to store output files (default: DataDictionaries/CleanedDataDictionaries)
  --project: Project identifier for the study (used to locate files and construct paths; required)
  --hdp_id: HEAL project ID; defaults to the project identifier if not provided
  --appl_id: APPL ID; if not provided, it will be determined from the metadata JSON (default: "")
  --temp_dir: Base directory for temporary files (default: tmp)

Example usage:
    ./scripts/convert_v2.py --input_directory input --output_directory output --project MyProject --hdp_id HDP12345 --appl_id 9877133 --temp_dir tmp
"""

import argparse
import logging
import os
import sys
from pathlib import Path
import shutil
import re
import yaml
import requests
import glob
from datetime import date

from healdata_utils import convert_to_vlmd

# Configure logging: adjust level and format as needed
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def create_metadata_yaml(yaml_path, hdp_id, appl_id, project_title, file_configs):
    """
    Create a metadata YAML file with project details and file-specific configurations.

    Parameters:
        yaml_path (Path): Path where the YAML file will be saved.
        hdp_id (str): HDP identifier.
        appl_id (str): Application identifier.
        project_title (str): Title of the project.
        file_configs (dict): Dictionary containing configuration info for each file.

    Returns:
        None
    """
    config = {
        'Project': {
            'HDP_ID': hdp_id,
            'APPL_ID': appl_id,
            'ProjectType': 'HEAL Research Programs',
            'LastModified': str(date.today()),
            'ProjectTitle': project_title,
            'Status': 'Draft'
        }
    }
    config.update(file_configs)

    with open(yaml_path, 'w') as f:
        yaml.dump(config, f)
    logging.info(f"Metadata YAML created at: {yaml_path}")


def create_directory_structure(output_directory, appl_id, hdp_id, project, temp_dir):
    """
    Create required directories for the application and temporary working space.

    The function creates:
      - An application directory under the output directory using the appl_id.
      - An HDP subdirectory (named by hdp_id) under the application directory.
      - Two subdirectories under the HDP directory: 'vlmd' for converted files and 'input' for original files.
      - A temporary working directory at {temp_dir}/{project}.

    Parameters:
        output_directory (str): Base directory for output.
        appl_id (str): Application identifier.
        hdp_id (str): HDP identifier.
        project (str): Project identifier (used to form the input subdirectory and temporary directory).
        temp_dir (str): Base directory for temporary files.

    Returns:
        tuple: (hdp_dir, temp_directory) as Path objects.
    """
    output_dir = Path(output_directory)
    appl_dir = output_dir / appl_id

    if not appl_dir.exists():
        appl_dir.mkdir(parents=True, exist_ok=True)
        logging.info(f"Created directory: {appl_dir}")
    else:
        logging.info(f"Directory already exists: {appl_dir}")

    hdp_dir = appl_dir / hdp_id
    vlmd_dir = hdp_dir / "vlmd"
    input_dir = hdp_dir / "input"
    if not hdp_dir.exists():
        hdp_dir.mkdir(parents=True, exist_ok=True)
        vlmd_dir.mkdir()
        input_dir.mkdir()
        logging.info(f"Created directories: {hdp_dir}, {vlmd_dir}, {input_dir}")
    else:
        logging.info(f"Directory already exists: {hdp_dir}")

    temp_directory = Path(temp_dir) / project
    if not temp_directory.exists():
        temp_directory.mkdir(parents=True, exist_ok=True)
        logging.info(f"Created temporary directory: {temp_directory}")
    else:
        logging.info(f"Temporary directory already exists: {temp_directory}")

    return hdp_dir, temp_directory


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


def determine_appl_id(hdp_id):
    """
    Retrieve metadata JSON from a URL and search for any occurrence of the key 'appl_id'.

    This function makes a request to:
        https://healdata.org/mds/metadata/{hdp_id}
    and recursively searches the returned JSON for 'appl_id'.

    Parameters:
        hdp_id (str): The HDP identifier.

    Returns:
        str: The application ID if found; otherwise, an empty string.
    """
    url = f"https://healdata.org/mds/metadata/{hdp_id}"
    logging.info(f"Retrieving appl_id from URL: {url}")
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        found_appl_id = search_for_key(data, "appl_id")
        if found_appl_id:
            logging.info(f"Found appl_id: {found_appl_id}")
            return found_appl_id
        else:
            logging.warning("appl_id not found in the JSON response.")
            return ""
    except requests.exceptions.RequestException as e:
        logging.error(f"Error retrieving appl_id: {e}")
        return ""


def fetch_project_metadata(appl_id, hdp_id):
    """
    Fetch project metadata from the HEAL data service for a given appl_id and hdp_id.

    The function constructs a query URL using the provided appl_id and retrieves metadata.
    It then looks for the matching HDP_ID in the JSON response and extracts the project title,
    updated HDP ID, and updated appl_id.

    Parameters:
        appl_id (str): Application identifier.
        hdp_id (str): HDP identifier.

    Returns:
        tuple: (project_title, updated_hdp_id, updated_appl_id)
    """
    query_url = f'https://healdata.org/mds/metadata?data=True&offset=0&nih_reporter.appl_id={appl_id}'
    logging.info(f"Query URL: {query_url}")
    project_title = 'NOT FOUND'

    try:
        response = requests.get(query_url)
        response.raise_for_status()
        response_json = response.json()
        logging.info(f"Total keys in response: {len(response_json)}")

        for key, value in response_json.items():
            if key == hdp_id:
                if 'nih_reporter' in value:
                    project_title = value['nih_reporter'].get('project_title', project_title)
                updated_hdp_id = value['gen3_discovery'].get('_hdp_uid', hdp_id)
                updated_appl_id = value['gen3_discovery'].get('appl_id', appl_id)
                logging.info(f"appl_id: {updated_appl_id} | hdp_id: {updated_hdp_id} | proj_title: {project_title}")
                return project_title, updated_hdp_id, updated_appl_id
            else:
                logging.info(f"Additional key in response: {key}")

        logging.warning("Matching HDP_ID not found in response metadata.")
        return project_title, hdp_id, appl_id

    except requests.exceptions.RequestException as e:
        logging.error(f"Error during request: {e}")
        return project_title, hdp_id, appl_id
    except ValueError as e:
        logging.error(f"Error processing JSON response: {e}")
        return project_title, hdp_id, appl_id


def process_files(file_list, temp_directory, hdp_dir, appl_id, project, project_title):
    """
    Process CSV files by converting them to VLMD format using supported input types.

    For each CSV file found, the function attempts conversion using "csv-data" first and then "redcap-csv".
    If conversion is successful, the resulting files are copied to the final directories, and configuration
    information is generated for use in the metadata YAML.

    Parameters:
        file_list (list): List of CSV file paths.
        temp_directory (Path): Temporary working directory for conversion outputs.
        hdp_dir (Path): Final HDP directory.
        appl_id (str): Application identifier.
        project (str): Project identifier.
        project_title (str): Title of the project.

    Returns:
        dict: file_configs with file names as keys and configuration dictionaries as values.
    """
    file_configs = {}

    for file_path in file_list:
        file_path = Path(file_path)
        file_name = file_path.name
        description = f'Filename: {file_name}'
        logging.info(description)

        if not file_name.endswith(".csv"):
            logging.info(f"Skipping non-CSV file: {file_path}")
            continue

        successful_conversion = False
        chosen_input_type = None
        data_dictionaries = None

        for input_type in ["csv-data", "redcap-csv"]:
            logging.info(f'>>> Processing file: {file_path} with input type: {input_type}')
            output_file = temp_directory / file_name.replace(".csv", ".vlmd.csv")

            try:
                data_dictionaries = convert_to_vlmd(
                    input_filepath=str(file_path),
                    output_filepath=str(output_file),
                    inputtype=input_type,
                    data_dictionary_props={"title": project_title, "description": description}
                )
            except FileExistsError:
                logging.info(f'{description} already processed')
                successful_conversion = True
                chosen_input_type = input_type
                break

            if data_dictionaries['errors']['csvtemplate']['valid'] and data_dictionaries['errors']['jsontemplate']['valid']:
                successful_conversion = True
                chosen_input_type = input_type
                break
            else:
                logging.info(f"Conversion with {input_type} failed, trying next input type if available.")

        if not successful_conversion:
            logging.error(f"File {file_path} requires additional cleaning before processing.")
            continue

        base_name = file_path.stem
        vlmd_outputs = glob.glob(str(temp_directory / f"{base_name}.*"))
        vlmd_json_dest = None

        for output in vlmd_outputs:
            dest_path = hdp_dir / "vlmd" / Path(output).name
            shutil.copy(output, dest_path)
            if output.endswith(".json"):
                vlmd_json_dest = dest_path

        input_dest = hdp_dir / "input" / f"{appl_id}_{file_name}"
        shutil.copyfile(file_path, input_dest)

        file_config = {
            'inputtype': chosen_input_type,
            'input_filepath': re.sub(".*input/",
                                     f"https://github.com/heal-data-stewards/heal-data-dictionaries/tree/main/data-dictionaries/{project}/input/",
                                     str(input_dest)),
            'output_filepath': re.sub(".*vlmd/",
                                      f"https://github.com/heal-data-stewards/heal-data-dictionaries/tree/main/data-dictionaries/{project}/vlmd/",
                                      str(vlmd_json_dest)) if vlmd_json_dest else "",
            'relative_input_filepath': re.sub(".*input/", "../input/", str(input_dest)),
            'relative_output_filepath': re.sub(".*vlmd/", "./", str(vlmd_json_dest)) if vlmd_json_dest else ""
        }
        file_configs[file_name] = file_config

    return file_configs


def main(args):
    """
    Main entry point for the HEAL Data Dictionaries Converter.

    This function parses command-line arguments, determines the appl_id if not provided,
    retrieves project metadata, sets up directory structures, processes CSV files, and
    generates a metadata YAML file if any valid files are processed.

    Parameters:
        args: Parsed command-line arguments.

    Returns:
        None
    """
    input_directory = args.input_directory
    output_directory = args.output_directory
    project = args.project
    temp_dir = args.temp_dir

    hdp_id = args.hdp_id if args.hdp_id else project
    appl_id = args.appl_id

    # Determine appl_id if not provided
    if appl_id == "":
        appl_id = determine_appl_id(hdp_id)
        if appl_id == "":
            logging.error("Could not determine appl_id from the metadata JSON.")
            sys.exit(1)

    project_title, hdp_id, appl_id = fetch_project_metadata(appl_id, hdp_id)

    hdp_dir, temp_directory = create_directory_structure(output_directory, appl_id, hdp_id, project, temp_dir)

    vlmd_dir = hdp_dir / "vlmd"
    input_dir = hdp_dir / "input"
    if any(vlmd_dir.iterdir()) or any(input_dir.iterdir()):
        logging.error("Output files already exist. If you want to reprocess the files, you must first remove the output files.")
        sys.exit(1)

    file_pattern = str(Path(input_directory) / project / "*.csv")
    file_list = glob.glob(file_pattern)
    logging.info(f"Found {len(file_list)} files for project {project}")
    logging.info(file_list)

    file_configs = process_files(file_list, temp_directory, hdp_dir, appl_id, project, project_title)

    if file_configs:
        metadata_yaml_path = hdp_dir / "vlmd" / "metadata.yaml"
        create_metadata_yaml(metadata_yaml_path, hdp_id, appl_id, project_title, file_configs)
        logging.info("CSV, VLMD, and metadata YAML files have been created and are ready for upload to GitHub.")
    else:
        logging.warning("No valid files processed; metadata YAML not created.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Process HEAL Data Dictionaries")
    parser.add_argument(
        "--input_directory",
        type=str,
        default="DataDictionaries/AssignedDataDictionaries",
        help="Directory containing input data dictionaries (default: DataDictionaries/AssignedDataDictionaries)"
    )
    parser.add_argument(
        "--output_directory",
        type=str,
        default="DataDictionaries/CleanedDataDictionaries",
        help="Directory to store output files (default: DataDictionaries/CleanedDataDictionaries)"
    )
    parser.add_argument(
        "--project",
        type=str,
        required=True,
        help="Project identifier for the study (used to locate files and construct paths)"
    )
    parser.add_argument(
        "--hdp_id",
        type=str,
        help="HEAL project ID; defaults to the project identifier if not provided"
    )
    parser.add_argument(
        "--appl_id",
        type=str,
        default="",
        help="APPL ID; if not provided, it will be determined from the metadata JSON (default: empty)"
    )
    parser.add_argument(
        "--temp_dir",
        type=str,
        default="tmp",
        help="Base directory for temporary files (default: tmp)"
    )
    args = parser.parse_args()

    main(args)
