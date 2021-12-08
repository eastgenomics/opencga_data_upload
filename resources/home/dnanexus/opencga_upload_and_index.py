#!/usr/bin/env python3

# import required libraries
import datetime
import os
import sys
import json
import logging
import argparse
import subprocess
from pyopencga.opencga_client import OpencgaClient
from pyopencga.opencga_config import ClientConfiguration
from subprocess import PIPE

# Define logs handler
logs = logging.getLogger()
logs.setLevel(logging.DEBUG)
handler = logging.FileHandler(filename='opencga_loader.log', mode='w')
format = logging.Formatter('%(asctime)s  %(name)s  %(levelname)s: %(message)s')
handler.setFormatter(format)
logs.addHandler(handler)

# Define status id
status_id = "name"  # Will be replaced by ID in the next release


def read_metadata(metadata_file):
    """
    Load the information in the metadata file
    :param metadata_file: JSON file containing the information to be fed to OpenCGA (mandatory fields: 'study')
    :return: dictionary with metadata params
    """
    metadata_dict = json.load(open(metadata_file, 'r'))
    return metadata_dict


def get_credentials(credentials_file):
    """
    Get the credentials from a JSON file to log into the OpenCGA instance
    :param credentials_file: JSON file containing the credentials and the host to connect to OpenCGA
    :return: dictionary with credentials and host
    """
    credentials_dict = json.load(open(credentials_file, 'r'))
    return credentials_dict


def connect_pyopencga(credentials):
    """
    Connect to pyopencga
    :param credentials: dictionary of credentials and host.
    """
    opencga_config_dict = {'rest': {'host': credentials['host']}}
    opencga_config = ClientConfiguration(opencga_config_dict)
    oc = OpencgaClient(opencga_config)
    oc.login(user=credentials['user'],
             password=credentials['password'])
    if oc.token is not None:
        logging.info("Succefully connected to pyopencga.\nTocken ID: {}".format(oc.token))
    else:
        logging.error("Failed to connect to pyopencga")
        sys.exit(0)
    return oc


def connect_cli(credentials, opencga_cli):
    """
    Connect OpenCGA CLI to instance
    :param opencga_cli: OpenCGA CLI
    :param credentials: dictionary of credentials and host
    """
    # Launch login on the CLI
    process = subprocess.run([opencga_cli, "users", "login", "-u", credentials['user']],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                             input=credentials['password'])
    logging.info(process.stdout)
    # Check that the login worked
    if process.stderr != "":
        logs.error("Failed to connect to OpenCGA CLI")
        sys.exit(0)


def check_file_status(oc, config, file_name):
    """
    Perform file checks. First if file has already been uploaded (file name exists in files.search) and if so, check
    if the file has been already indexed and annotated.
    :param oc: openCGA client
    :param config: configuration dictionary
    :param file_name: name of the file that wants to be uploaded
    :return: returns three booleans indicating whether the file has been uploaded, indexed and annotated
    """

    # Init check variables to False
    uploaded = False
    indexed = False
    annotated = False

    # Check if file has been uploaded
    logs.info("Checking status of the file")
    try:
        # Query file search in OpenCGA
        file_search = oc.files.search(study=config['study'], name=file_name)

        # File does not exist
        if file_search.get_num_results() == 0:
            logs.info("File {} does not exist in the OpenCGA study {}.".format(file_name, config['study']))
        # File exists and there's no more than one file with that name
        elif file_search.get_num_results() == 1:
            file_status = file_search.get_result(0)['internal']['status'][status_id]
            # file_status = file_search.get_result(0)['internal']['variant']['status'][status_id]
            if file_status == "READY":
                uploaded = True
                logs.info("File {} already exists in the OpenCGA study {}. This file will not be uploaded again. "
                          "Path to file: {}".format(file_name, config['study'], file_search.get_result(0)['path']))
                # Check if file has been indexed (only for those already uploaded)
                if file_search.get_result(0)['internal']['index']['status'][status_id] == "READY":
                    # if file_search.get_result(0)['internal']['variant']['index']['status'][status_id] == "READY":
                    indexed = True
                    if file_search.get_result(0)['internal']['annotationIndex']['status'][status_id] == "READY":
                        # if file_search.get_result(0)['internal']['variant']['annotationIndex']['status'][status_id] == "READY":
                        annotated = True
                    # TODO: Add extra checks for variant index and sample index
                    # variant:cd ...
                    # annotationIndex
                    # secondaryIndex (no lo vamos a hacer)

                    # multifile upload not supported
                    # sample: !!! get sample ID from file ^^^
                    # index
                    # genotypeIndex -- operation/variant/sample/index
                    # annotationIndex
            else:
                # File exists but status is not READY - Needs to be uploaded again
                logs.info("File {} already exist in the OpenCGA study {} but status is {}. This file will be "
                          "uploaded again.".format(file_name, config['study'], file_status))
        # There is more than one file with this name in this study!
        else:
            uploaded = True
            logs.error("File {} has already been indexed in the OpenCGA study {}.\n"
                       "No further processing will be done.".format(file_name, config['study']))
            sys.exit(0)
    except Exception as e:
        logs.error(exc_info=e)
        sys.exit(0)
    return uploaded, indexed, annotated


def check_upload_status(oc, metadata, file_name, dnanexus_fid):
    """
    Check if file has already been uploaded (file name exists in files.search). If the file has been already uploaded
    checks that the file in DNA nexus has the same file ID as the one stored in OpenCGA.
    :param oc: openCGA client
    :param metadata: metadata dictionary
    :param file_name: name of the file that wants to be uploaded
    :param dnanexus_fid: DNA Nexus file ID
    :return: returns three booleans indicating whether the file has been uploaded
    """
    # Initialise variable to False
    uploaded = None

    # Check if file has been uploaded
    try:
        # Query file search in OpenCGA
        file_search = oc.files.search(study=metadata['study'], name=file_name)
        # File does not exist
        if file_search.get_num_results() == 0:
            uploaded = False
            logs.info("File {} does not exist in the OpenCGA study {}.".format(file_name, metadata['study']))
        # File exists and there's no more than one file with that name
        elif file_search.get_num_results() == 1:
            file_status = file_search.get_result(0)['internal']['status'][status_id]
            # file_status = file_search.get_result(0)['internal']['variant']['status'][status_id]
            logs.info("Upload status: {}".format(file_status))
            if file_status == "READY":
                uploaded = True
                logs.info("File {} already exists in the OpenCGA study {}. "
                          "Path to file: {}".format(file_name, metadata['study'], file_search.get_result(0)['path']))
                # TODO: Add check of file ID
            else:
                uploaded = False
                # File exists but status is not READY - Needs to be uploaded again
                logs.info("File {} already exist in the OpenCGA study {} but status is {}. This file will be "
                          "uploaded again.".format(file_name, metadata['study'], file_status))
                # There is more than one file with this name in this study!
        else:
            logs.error("More than one file in OpenCGA with this name {} in study {}".format(file_name,
                                                                                            metadata['study']))
            sys.exit(0)
    except Exception as e:
        logs.error(exc_info=e)
        sys.exit(0)
    return uploaded


def check_index_status(oc, metadata, file_name):
    """
    Check if file has already been indexed in OpenCGA
    :param oc: openCGA client
    :param metadata: metadata dictionary
    :param file_name: name of the file to be checked
    :return: returns three booleans indicating whether the file has been indexed
    """
    # Init check variables to False
    indexed = None

    # Check if file has been uploaded
    try:
        # Query file search in OpenCGA
        file_search = oc.files.search(study=metadata['study'], name=file_name)

        # File does not exist
        if file_search.get_num_results() == 0:
            logs.error("File {} does not exist in the OpenCGA study {}.".format(file_name, metadata['study']))
            sys.exit(0)
        # File exists and there's no more than one file with that name
        elif file_search.get_num_results() == 1:
            file_status = file_search.get_result(0)['internal']['status'][status_id]
            # file_status = file_search.get_result(0)['internal']['variant']['status'][status_id]
            logs.info("Index status: {}".format(file_search.get_result(0)['internal']['index']['status'][status_id]))
            if file_status == "READY":
                # Check if file has been indexed (only for those already uploaded)
                if file_search.get_result(0)['internal']['index']['status'][status_id] == "READY":
                    # if file_search.get_result(0)['internal']['variant']['index']['status'][status_id] == "READY":
                    indexed = True
                else:
                    indexed = False
            else:
                # File exists but status is not READY - Needs to be uploaded again
                logs.info("File {} already exist in the OpenCGA study {} but status is {}.".format(file_name,
                                                                                                   metadata['study'],
                                                                                                   file_status))
                indexed = False
        # There is more than one file with this name in this study!
        else:
            logs.error("More than one file in OpenCGA with this name {} in study {}".format(file_name,
                                                                                            metadata['study']))
            sys.exit(0)
    except Exception as e:
        logs.error(exc_info=e)
        sys.exit(0)
    return indexed


def check_annotation_status(oc, metadata, file_name):
    """
    Check if file has already been annotated in OpenCGA
    :param oc: openCGA client
    :param metadata: metadata dictionary
    :param file_name: name of the file to be checked
    :return: returns three booleans indicating whether the file has been indexed
    """
    # Init check variables to False
    annotated = None

    try:
        # Query file search in OpenCGA
        file_search = oc.files.search(study=metadata['study'], name=file_name)

        # File does not exist
        if file_search.get_num_results() == 0:
            logs.error("File {} does not exist in the OpenCGA study {}.".format(file_name, metadata['study']))
            sys.exit(0)
        # File exists and there's no more than one file with that name
        elif file_search.get_num_results() == 1:
            file_status = file_search.get_result(0)['internal']['status'][status_id]
            # file_status = file_search.get_result(0)['internal']['variant']['status'][status_id]
            if file_status == "READY":
                # Check if file has been indexed (only for those already uploaded)
                if file_search.get_result(0)['internal']['index']['status'][status_id] == "READY":
                    # if file_search.get_result(0)['internal']['variant']['index']['status'][status_id] == "READY":
                    if file_search.get_result(0)['internal']['annotationIndex']['status'][status_id] == "READY":
                        # if file_search.get_result(0)['internal']['variant']['annotationIndex']['status'][status_id] == "READY":
                        logs.info("Index status: {}".format(
                            file_search.get_result(0)['internal']['annotationIndex']['status'][status_id]))
                        annotated = True
            else:
                annotated = False
                # File exists but status is not READY - Needs to be uploaded again
                logs.info("File {} already exist in the OpenCGA study {} but status is {}.".format(file_name,
                                                                                                   metadata['study'],
                                                                                                   file_status))
        # There is more than one file with this name in this study!
        else:
            logs.error("More than one file in OpenCGA with this name {} in study {}".format(file_name,
                                                                                            metadata['study']))
            sys.exit(0)
    except Exception as e:
        logs.error(msg=e)
        sys.exit(0)
    return annotated


def upload_file(opencga_cli, metadata, file, file_path="data/"):
    """
    Uploads a file to the OpenCGA instance and stores it in the file path
    :param opencga_cli: OpenCGA CLI
    :param metadata: metadata dictionary
    :param file: VCF file to upload
    :param file_path: directory inside OpenCGA where the file should be stored (default: data/)
    """
    # Run upload using the bash CLI
    process = subprocess.Popen([opencga_cli, "files", "upload", "--input", file, "--study", metadata['study'],
                                "--catalog-path", file_path, "--parents"], stdout=PIPE, stderr=PIPE, text=True)
    process.wait()  # Wait until the execution is complete to continue with the program
    stdout, stderr = process.communicate()
    if stderr != "":
        logs.error(str(stderr))
        sys.exit(0)
    else:
        logs.info("File uploaded successfully. Path to file in OpenCGA catalog: {}".format(stdout.split('\t')[18]))
        logs.info("\n" + stdout)


def index_file(oc, metadata, file):
    """
    Indexes a VCF that has already been uploaded to OpenCGA
    :param oc: OpenCGA client
    :param metadata: metadata dictionary
    :param file: name of the VCF file already uploaded in OpenCGA
    """
    index_job = oc.variants.run_index(study=metadata['study'], data={"file": file})
    logs.info("Indexing file {} with job ID: {}".format(file, index_job.get_result(0)['id']))
    try:
        oc.wait_for_job(response=index_job.get_response(0))
    except ValueError as ve:
        logs.error("OpenCGA failed to index the file. {}".format(ve))
        sys.exit(0)


def annotate_variants(oc, metadata):
    """
    Launches an OpenCGA job to force the annotation of any new variants added to the database.
    :param oc: OpenCGA client
    :param metadata: metadata dictionary
    """
    annotate_job = oc.variant_operations.index_variant_annotation(study=metadata['study'], data={})
    logs.info("Annotating new variants in study {} with job ID: {}".format(metadata['study'],
                                                                           annotate_job.get_result(0)['id']))
    try:
        oc.wait_for_job(response=annotate_job.get_response(0))
    except ValueError as ve:
        logs.error("OpenCGA annotation job failed. {}".format(ve))
        sys.exit(0)


if __name__ == '__main__':
    # Set the arguments of the command line
    parser = argparse.ArgumentParser(description=' Index VCFs from DNANexus into OpenCGA')
    parser.add_argument('--metadata', help='JSON file containing the metadata (minimum required information: "study")')
    parser.add_argument('--credentials', help='JSON file with credentials and host to access OpenCGA')
    parser.add_argument('--cli', help='Path to OpenCGA cli')
    parser.add_argument('--vcf', help='Input vcf file')
    parser.add_argument('--dnanexus_fid', help='DNA nexus file ID')
    args = parser.parse_args()

    # Check the location of the OpenCGA CLI
    if not os.path.isfile(args.cli):
        logs.error("OpenCGA CLI not found.")
        sys.exit(0)
    opencga_cli = args.cli

    # Read metadata file
    metadata = read_metadata(metadata_file=args.metadata)

    # Read credentials file
    credentials = get_credentials(credentials_file=args.credentials)

    # Login OpenCGA CLI
    connect_cli(credentials=credentials, opencga_cli=opencga_cli)

    # Create pyopencga client
    oc = connect_pyopencga(credentials=credentials)

    # Get today's date to store the file in a directory named as "YearMonth" (e.g. 202112 = December 2021)
    date_folder = datetime.date.today().strftime("%Y%m")
    file_path = "data/" + date_folder

    # Check the status of the file and execute the necessary actions
    # uploaded, indexed, annotated = check_file_status(oc=oc, config=config, file_name=os.path.basename(args.vcf))

    # Check upload status
    uploaded = check_upload_status(oc=oc, metadata=metadata, file_name=os.path.basename(args.vcf),
                                   dnanexus_fid=args.dnanexus_fid)
    if uploaded is not None:
        if not uploaded:
            upload_file(opencga_cli=opencga_cli, metadata=metadata, file=args.vcf, file_path=file_path)

    # Check index status
    indexed = check_index_status(oc=oc, metadata=metadata, file_name=os.path.basename(args.vcf))
    if indexed is not None:
        if not indexed:
            index_file(oc=oc, metadata=metadata, file=os.path.basename(args.vcf))

    # Check variant annotation status
    # NOTE: Cannot be done at the moment
    # annotated = check_annotation_status(oc=oc, metadata=metadata, file_name=os.path.basename(args.vcf))
    annotated = False
    if annotated is not None:
        if not annotated:
            annotate_variants(oc=oc, metadata=metadata)

    # # Check again the status of the file
    # uploaded, indexed, annotated = check_file_status(oc=oc, config=config, file_name=os.path.basename(args.vcf))
    # if uploaded and indexed and annotated:
    #     logs.info("File {} has been successfully uploaded, indexed and annotated.")
    # else:
    #     logs.error("Something went wrong. Status of file {}:\n\t- uploaded: {}\n\t- indexed: {}\n\t- annotated: {}\n"
    #                "Please check the logs to identify the problem.".format(args.vcf, uploaded, indexed, annotated))
    handler.close()
