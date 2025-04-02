# HEAL Data Dictionaries Converter

This script processes CSV files containing HEAL Data Dictionaries by converting them
to VLMD format and generating a metadata YAML file. It retrieves project metadata from
the HEAL data service, creates the required directory structure, and processes each CSV
file. The output files (VLMD files, metadata YAML, etc.) are then prepared for further
processing or upload to GitHub.

## Features

- CSV to VLMD Conversion: Converts CSV files to VLMD format using supported input types.
- Metadata Retrieval: Fetches project metadata from the HEAL data service.
- Dynamic Directory Structure: Creates output directories for converted files and
  temporary workspaces.
- Flexible Argument Parsing: Key parameters can be provided via command-line arguments.
- Automatic APPL ID Determination: Retrieves the APPL ID from the metadata JSON if not
  provided.

## Command-Line Arguments

--input_directory
    Type: str
    Default: DataDictionaries/AssignedDataDictionaries
    Description: Base directory containing input CSV files. The script expects a
    subdirectory within this directory named after the project identifier.

--output_directory
    Type: str
    Default: DataDictionaries/CleanedDataDictionaries
    Description: Directory where output files (VLMD, YAML, etc.) are stored.

--project
    Type: str
    Required: Yes
    Description: Project identifier used to locate CSV files in the input directory
    and to construct output paths.

--hdp_id
    Type: str
    Default: Defaults to the project identifier if not provided.
    Description: HEAL project ID used in querying metadata and naming directories.

--appl_id
    Type: str
    Default: "" (empty string)
    Description: APPL ID. If not provided, the script retrieves the JSON metadata from
    the URL and searches for the first occurrence of appl_id.

--temp_dir
    Type: str
    Default: tmp
    Description: Base directory for temporary files. A temporary directory is created
    as {temp_dir}/{project}.

## Usage Example

    ./scripts/convert_v2.py --input_directory input \
      --output_directory output \
      --project MyStudy \
      --hdp_id HDP12345 \
      --appl_id 9877133 \
      --temp_dir tmp

If --appl_id is omitted, the script retrieves the metadata from:

    https://healdata.org/mds/metadata/{hdp_id}

and extracts the APPL ID automatically.

## Process Flow

Metadata Retrieval:
    The script constructs a query URL using the provided or automatically determined
    APPL ID and retrieves the JSON metadata. If no APPL ID is supplied, it fetches the
    JSON using the HDP ID and searches for the key appl_id.

Directory Structure Setup:
    Creates an output directory under the APPL ID.
    Under the output directory, creates an HDP subdirectory that includes two folders:
    vlmd (for converted files) and input (for the original CSV files).
    A temporary directory is created at {temp_dir}/{project}.

CSV File Processing:
    Searches for CSV files in the input directory under a subfolder named after the
    project identifier. Each CSV file is processed using supported conversion types.

Metadata YAML Generation:
    If file conversions are successful, a metadata YAML file is generated in the vlmd
    directory with project and file-specific configuration details.

Final Output:
    The resulting VLMD files, original CSV files (copied to the input directory), and
    metadata YAML are ready for further use.

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
