#!/usr/bin/env python3

# import required libraries
import os
import sys
import yaml
import json
import logging
import argparse
import subprocess
from pyopencga.opencga_client import OpencgaClient
from pyopencga.opencga_config import ClientConfiguration
from subprocess import PIPE

# Define logs handler
logs = logging.getLogger()
logs.setLevel(logging.INFO)
handler = logging.FileHandler(filename='opencga_loader.log', mode='w')
format = logging.Formatter('%(asctime)s  %(name)s  %(levelname)s: %(message)s')
handler.setFormatter(format)
logs.addHandler(handler)

# Define status id
status_id = "name"  # Will be replaced by ID in the next release


def read_metadata(metadata_file):
    """
    Load the metadata.json file
    :param metadata_file:
    :return: dictionary with metadata params
    """
    metadata = json.load(open(metadata_file))
    return metadata

## This file is taken from the main.sh code
# def read_credentials(cred_file):
#     """
#     Load the credentials file
#     :param credentials file:
#     :return: dictionary with cred params
#     """
#     credentials = json.load(open(cred_file))
#     return credentials


def connect_pyopencga(user, password, host):
    """
    Connect to pyopencga
    :param config: dictionary of parameters.
    """
    opencga_config_dict = {'rest': {'host': host}}
    opencga_config = ClientConfiguration(opencga_config_dict)
    oc = OpencgaClient(opencga_config)
    oc.login(user=user,
             password=password)
    if oc.token is not None:
        logging.info("Succefully connected to pyopencga.\nTocken ID: {}".format(oc.token))
    else:
        logging.error("Failed to connect to pyopencga")
        sys.exit(0)
    return oc


def connect_cli(user, password, opencga_cli):
    """
    Connect OpenCGA CLI to instance
    :param opencga_cli: OpenCGA CLI
    :param config: configuration dictionary
    """
    # Launch login on the CLI
    process = subprocess.run([opencga_cli, "users", "login", "-u", user],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                             input=password)
    logging.info(process.stdout)
    # Check that the login worked
    if process.stderr != "":
        logs.error("Failed to connect to OpenCGA CLI")
        sys.exit(0)


def check_file_status(oc, study, file_name):
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
        file_search = oc.files.search(study=study, name=file_name)

        # File does not exist
        if file_search.get_num_results() == 0:
            logs.info("File {} does not exist in the OpenCGA study {}.".format(file_name, study))
        # File exists and there's no more than one file with that name
        elif file_search.get_num_results() == 1:
            file_status = file_search.get_result(0)['internal']['status'][status_id]
            # file_status = file_search.get_result(0)['internal']['variant']['status'][status_id]
            if file_status == "READY":
                uploaded = True
                logs.info("File {} already exists in the OpenCGA study {}. This file will not be uploaded again. "
                          "Path to file: {}".format(file_name, study, file_search.get_result(0)['path']))
                # Check if file has been indexed (only for those already uploaded)
                if file_search.get_result(0)['internal']['index']['status'][status_id] == "READY":
                # if file_search.get_result(0)['internal']['variant']['index']['status'][status_id] == "READY":
                    indexed = True
                    if file_search.get_result(0)['internal']['annotationIndex']['status'][status_id] == "READY":
                    # if file_search.get_result(0)['internal']['variant']['annotationIndex']['status'][status_id] == "READY":
                        annotated = True
                    # TODO: Add extra checks for variant index and sample index
                        # variant:cd ...
                            #annotationIndex
                            #secondaryIndex (no lo vamos a hacer)

                        # multifile upload not supported
                        # sample: !!! get sample ID from file ^^^
                            # index
                            # genotypeIndex -- operation/variant/sample/index
                            # annotationIndex
            else:
                # File exists but status is not READY - Needs to be uploaded again
                logs.info("File {} already exist in the OpenCGA study {} but status is {}. This file will be "
                          "uploaded again.".format(file_name, study, file_status))
        # There is more than one file with this name in this study!
        else:
            uploaded = True
            logs.error("File {} has already been indexed in the OpenCGA study {}.\n"
                       "No further processing will be done.".format(file_name, study))
            sys.exit(0)
    except Exception as e:
        logs.error(exc_info=e)
        sys.exit(0)
    return uploaded, indexed, annotated


def upload_file(opencga_cli, study, file):
    """
    Uploads a file to the OpenCGA instance
    :param opencga_cli: OpenCGA CLI
    :param config: configuration dictionary
    """
    process = subprocess.Popen([opencga_cli, "files", "upload", "--input", file, "--study", study],
                               stdout=PIPE, stderr=PIPE, text=True)
    process.wait()  # Wait until the execution is complete to continue with the program
    stdout, stderr = process.communicate()
    if stderr != "":
        logs.error(str(stderr))
        sys.exit(0)
    else:
        logs.info("File uploaded successfully. Path to file in OpenCGA catalog: {}".format(stdout.split('\t')[18]))
        logs.info(stdout)


def annotate_file(oc, study):
    """
    Launches an OpenCGA job to force the annotation of any new variants added to the database.
    :param oc: OpenCGA client
    :param config: configuration dictionary
    """
    annotate_job = oc.variant_operations.index_variant_annotation(study=study, data={})
    logs.info("Annotating new variants in study {} with job ID: {}".format(study,
                                                                           annotate_job.get_result(0)['id']))
    try:
        oc.wait_for_job(response=annotate_job.get_response(0))
    except ValueError as ve:
        logs.error("OpenCGA annotation job failed. {}".format(ve))
        sys.exit(0)
    # TODO: Add job logs to our logs


def index_file(oc, study, file):
    """
    Indexes a VCF that has already been uploaded to OpenCGA
    :param oc: OpenCGA client
    :param config: configuration dictionary
    :param file: name of the VCF file already uploaded in OpenCGA
    """
    index_job = oc.variants.run_index(study=study, data={"file": file})
    logs.info("Indexing file {} with job ID: {}".format(file, index_job.get_result(0)['id']))
    try:
        oc.wait_for_job(response=index_job.get_response(0))
    except ValueError as ve:
        logs.error("OpenCGA failed to index the file. {}".format(ve))
        sys.exit(0)
    # TODO: Add job logs to our logs

    # job_info = oc.jobs.info(study=study, jobs=index_job.get_result(0)['id'])
    # job_status = job_info.get_result(0)['internal']['status'][status_id]
    # if job_status == "DONE":
    #     oc.variant_operations.index_variant_annotation()


if __name__ == '__main__':

    # Set the arguments of the command line
    parser = argparse.ArgumentParser(description=' Index VCFs from DNANexus into OpenCGA')
    parser.add_argument('--metadata', help='Path to metadata file')
    parser.add_argument('--cli', help='OpenCGA cli')
    parser.add_argument('--user', help='OpenCGA user')
    parser.add_argument('--password', help='OpenCGA password')
    parser.add_argument('--vcf', metavar='vcf', help='Input vcf file')
    args = parser.parse_args()

    # Get location of the script to define the default location of the config file
    metadata = None
    if args.metadata is not None:
        metadata = args.metadata
        logs.info("Input metadata json: {}".format(metadata))
    else:
        # If cli is not passed as an argument nor is in the path, raise and error and exit.
        logs.error("No metadata.json file found")
        sys.exit(0)

    # Define location of the OpenCGA client
    opencga_cli = None
    if args.cli is not None:
        # If cli is passed as an argument, use this
        opencga_cli = args.cli
        logs.info("Input OpenCGA CLI: {}".format(opencga_cli))
    else:
        # If cli is not passed as an argument raise and error and exit.
        logs.error("OpenCGA CLI not found.")
        sys.exit(0)

    # Read metadata file AND define study variable
    metadata = read_metadata(metadata_file=args.metadata)
    if metadata['study'] is not None:
        study = metadata['study']
        logs.info("OpenCGA study to operate {}".format(study))
    else:
        logs.error("OpenCGA study not found in metadata file. Study id is mandatory.")
        sys.exit(0)

    # Login OpenCGA CLI
    connect_cli(user=args.user, password=args.password, opencga_cli=opencga_cli)

    # Create pyopencga client
    oc = connect_pyopencga(user=args.user, password=args.password, host=args.host)

    # COMMENT FROM HERE
    # Check if file has been already uploaded and indexed
    uploaded, indexed, annotated = check_file_status(oc=oc, study=study, file_name=os.path.basename(args.vcf))

    # Depending on the status of the file we will upload it and/or index it
    if not uploaded:
        # Upload file
        upload_file(opencga_cli=opencga_cli, study=study, file=args.vcf)
    if not indexed:
        # Index file
        index_file(oc=oc, study=study, file=os.path.basename(args.vcf))
    if not annotated:
        # Annotate file
        annotate_file(oc=oc, study=study)

    # Check again the status of the file
    uploaded, indexed, annotated = check_file_status(oc=oc, study=study, file_name=os.path.basename(args.vcf))
    if uploaded and indexed and annotated:
        logs.info("File {} has been successfully uploaded, indexed and annotated.")
    else:
        logs.error("Something went wrong. Status of file {}:\n\t- uploaded: {}\n\t- indexed: {}\n\t- annotated: {}\n"
                   "Please check the logs to identify the problem.".format(args.vcf, uploaded, indexed, annotated))

    handler.close()
