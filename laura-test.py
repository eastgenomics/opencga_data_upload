#!/usr/bin/env python3

# import required libraries
import datetime
import os
import sys
import yaml
import logging
import argparse
import subprocess
from pyopencga.opencga_client import OpencgaClient
from pyopencga.opencga_config import ClientConfiguration
from subprocess import PIPE


# Define logs
logs = logging.getLogger()
logs.setLevel(logging.DEBUG)
handler = logging.FileHandler('opencga_loader.log')
format = logging.Formatter('%(asctime)s  %(name)s  %(levelname)s: %(message)s')
handler.setFormatter(format)
logs.addHandler(handler)


def read_config(config_file):
    """
    Load the configuration file
    :param config_file:
    :return: dictionary with config params
    """
    config = yaml.load(open(config_file), Loader=yaml.FullLoader)
    return config


def connect_pyopencga(config):
    """
    Connect to pyopencga
    :param config: dictionary of parameters.
    """
    opencga_config_dict = {'rest': {'host': config['rest']['host']}}
    opencga_config = ClientConfiguration(opencga_config_dict)
    oc = OpencgaClient(opencga_config)
    oc.login(user=config['credentials']['user'],
             password=config['credentials']['password'])
    if oc.token is not None:
        logging.info("Succefully connected to pyopencga.\nTocken ID: {}".format(oc.token))
    else:
        logging.error("Failed to connect to pyopencga")
        sys.exit(0)
    return oc


def connect_cli(config, opencga_cli):
    # Launch login on the CLI
    process = subprocess.run([opencga_cli, "users", "login", "-u", config['credentials']['user']],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                             input=config['credentials']['password'])
    logging.info(process.stdout)
    # Check that the login worked
    if process.stderr != "":
        logs.error("Failed to connect to OpenCGA CLI")
        sys.exit(0)


def check_file_status(oc, config, file_name):
    """
    Perform file checks. First if file has already been uploaded (file name exists in files.search) and if so, check
    if the file has been already indexed.
    :param oc: openCGA client
    :param config: configuration dictionary
    :param file_name:
    :return:
    """
    logs.info("Performing checks...")
    uploaded = False
    indexed = False
    annotated = False
    # Check if file has been uploaded
    try:
        # Query file search in opencga
        file_search = oc.files.search(study=config['study'],
                                      name=file_name)
        # File does not exist
        if file_search.get_num_results() == 0:
            logs.info("File {} does not exist in the opencga study {}.".format(file_name, config['study']))
        # File exists and there's no more than one file with that name
        elif file_search.get_num_results() == 1:
            file_status = file_search.get_result(0)['internal']['status']['name']
            if file_status == "READY":
                uploaded = True
                logs.info("File {} already exist in the opencga study {}. This file will not be uploaded again.\n"
                             "Path to file: {}".format(file_name, config['study'], file_search.get_result(0)['path']))
                # Check if file has been indexed (only for those already uploaded)
                if file_search.get_result(0)['internal']['index']['status']['name'] == "READY":
                    indexed = True
            else:
                # File exists but status is not READY - Needs to be uploaded again
                logs.info("File {} already exist in the opencga study {} but status is {}. This file will be "
                             "uploaded again.".format(file_name, config['study'], file_status))
        elif file_search.get_num_results() > 1:
            uploaded = True
            logs.info("File {} has already been indexed in the opencga study {}.\n"
                         "No further processing will be done.".format(file_name, config['study']))
    except Exception as e:
        logs.error(exc_info=e)
        sys.exit(0)
    return uploaded, indexed, annotated


def upload_file(opencga_cli, config, file):
    """
    Uploads a file to the opencga instance
    """
    uploaded = False
    process = subprocess.Popen([opencga_cli, "files", "upload", "--input", file, "--study", config['study']],
                               stdout=PIPE, stderr=PIPE, text=True)
    process.wait()  # Wait until the execution is complete to continue with the program
    stdout, stderr = process.communicate()
    if stderr != "":
        logs.error(str(stderr))
        sys.exit(0)
    else:
        logs.info("File uploaded successfully. Path to file in opencga catalog: {}".format(stdout.split('\t')[18]))
        logs.info(stdout)
        uploaded = True
    return uploaded


def index_file(opencga_cli, config, file):
    resp = oc.indexVariant(file)
    oc.wait_for_job(resp.id)
    job = oc.jobs.info(resp.id)
    if (job.intenral.status === "DONE") {
        result = oc.variant_operations.annotate(...)
        ...


    } else {

    }


if __name__ == '__main__':
    # Get location of the script to define the default location of the config file
    config_default = None
    if os.path.isfile(os.path.dirname(__file__) + "/resources/config.yml"):
        config_default = os.path.dirname(__file__) + "/resources/config.yml"
    # Define location of the opencga client
    opencga_cli = None
    if os.path.isfile(os.path.dirname(__file__) + "/src/opencga-cli/opencga-client-2.1.0-rc2/bin/opencga.sh"):
        opencga_cli = os.path.dirname(__file__) + "/src/opencga-cli/opencga-client-2.1.0-rc2/bin/opencga.sh"
    else:
        logs.error("OpenCGA CLI not found.")
        sys.exit(0)

    # Set the arguments of the command line
    # Note: here we are assuming that the files will be uploaded in the root of the catalog file structure.
    # If not, more args and code
    parser = argparse.ArgumentParser(description=' Index VCFs from DNANexus into OpenCGA')
    parser.add_argument('--config', help='Path to configuration file', default=config_default)
    parser.add_argument('--input', metavar='input', help='input vcf file')
    args = parser.parse_args()

    # Read config file
    config = read_config(config_file=args.config)

    # Login opencga CLI
    connect_cli(config=config, opencga_cli=opencga_cli)

    # Create pyCGA client
    oc = connect_pyopencga(config=config)

    # Check if file has been already uploaded and indexed
    uploaded, indexed, annotated = check_file_status(oc=oc, config=config, file_name=os.path.basename(args.input))

    # Depending on the status of the file we will upload it and/or index it
    #if not uploaded:
        # Upload file
    file_uploaded = upload_file(opencga_cli=opencga_cli, config=config, file=args.input)
    index_file(opencga_cli=opencga_cli, config=config, file=args.input)

    #     # Index file
    # elif uploaded and not indexed:
    #     # index file
    # elif uploaded and indexed and not annotated:
    #     # annotate?
    # elif uploaded and indexed and annotated:
    #     # do nothing
    handler.close()

