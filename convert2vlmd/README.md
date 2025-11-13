# HEAL Data Dictionaries Converter

The script `script/convert2vlmd.py` processes REDCap data dictionary CSVs and Stata files containing HEAL Data Dictionaries by converting them to VLMD format and generating corresponding metadata YAML file. It retrieves project metadata from the HEAL metadata service, creates the required directory structure, and processes each CSV/Stata file. The output files (VLMD files, metadata YAML, etc.) are then prepared for further processing or upload to GitHub. The primary aim is to create files that are ready to be ingested by the [HEAL Data Platform](https://healdata.org/portal).

Original code can be found here: https://github.com/uc-cdis/heal-data-dictionaries/tree/main/notebooks/reference/convert2vlmd.py

## Features

- REDCap CSV to VLMD Conversion: Converts CSV files to VLMD format using supported input types.
- Stata to VLMD Conversion
- Metadata Retrieval: Fetches project metadata from the HEAL metadata service.
- Dynamic Directory Structure: Creates output directories for converted files and
  temporary workspaces.
- Flexible Argument Parsing: Key parameters can be provided via command-line arguments.
- Automatic APPL ID Determination: Retrieves the APPL ID from the metadata JSON if not
  provided.

## Command-Line Arguments

--clean_study_directory
Type: str
Required: Yes
Description: Directory containing input data dictionary files. This directory will contain one file for every data dictionary in the study, which is ready to be ingested by [healdata-utils](https://heal.github.io/healdata-utils/) tool.

--output_directory
Type: str
Required: Yes
Description: All output files (VLMD, YAML, etc.) will be stored in <output_director>/data-dictionaries/<HDP_ID> or <output_director>/data-dictionaries/<HDP_ID> if <project> is provided.
A directory for every input file will be generated with a vlmd.json, vlmd.csv and a corresponding metadata.yaml.

--project
Type: str
Default: ''
Description: If povided, output files will be written to <output_directory>/data-dictionaries/<project> folder

--hdp_id
Type: str
Required: Yes
Description: HEAL project ID (HDPID) used in querying metadata and naming directories.

--appl_id
Type: str
Default: "" (empty string)
Description: APPL ID. If not provided, the APPL ID is extracted from the MDS service query on HDPID.

--project_type
Type: str
Default: Research Programs
Description: Study type (eg.HEAL Research Programs, HEAL Research Networks, HEAL Study), (default="Research Programs")

--overwrite
Type: flag
Default: False
Description: If provided, any outputs previously generated will be overwritten. If not, an error will be generated when existing output files are found.

## Usage Example

Example 1:
./scripts/convert2vlmd.py --clean_study_directory <path-to-dir-with-study-files> \
 --output_directory <path-to-output-directory> \
 --hdp_id HDP12345 \
 --project_type "Resaerch Programs"

The script retrieves appl_id from the metadata from: https://healdata.org/mds/metadata/{hdp_id} and extracts the APPL ID automatically.

The outputs will be written to <path-to-output-directory>/data-dictionaries/HDP12345

Example 2:
./scripts/convert2vlmd.py --clean_study_directory <path-to-dir-with-study-files> \
 --output_directory <path-to-output-directory> \
 --hdp_id HDP12345 \
 --appl_id 9877133 \
 --project HEALResearchProgram
--project_type "Resaerch Programs"
The outputs will be written to <path-to-output-directory>/data-dictionaries/HEALResearchProgram

## Dependencies

- Python 3.x
- requests
- pyyaml
- Standard Python modules: argparse, logging, pathlib, shutil, glob, re, datetime
- healdata_utils module (provides the convert_to_vlmd function)

Ensure that all required modules are installed in your environment.

## License

Specify the license information here if applicable.

## Contact

For more information or support, please contact the HEAL Data Stewards.
