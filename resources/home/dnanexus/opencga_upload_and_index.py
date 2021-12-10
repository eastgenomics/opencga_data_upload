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


# Define logger handlers (one file for logs and one for errors)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
# create file handler which logs INFO messages
oh = logging.FileHandler('opencga_loader.out')
oh.setLevel(logging.DEBUG)
# create file handler which logs ERROR messages
eh = logging.FileHandler('opencga_loader.err')
eh.setLevel(logging.ERROR)
# create stream handler which logs INFO messages
console = logging.StreamHandler(stream=sys.stdout)
console.setLevel(logging.INFO)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s: %(message)s')
eh.setFormatter(formatter)
oh.setFormatter(formatter)
console.setFormatter(formatter)
# add the handlers to logger
logger.addHandler(eh)
logger.addHandler(oh)
logger.addHandler(console)

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
        logger.info("Succefully connected to pyopencga.\nTocken ID: {}".format(oc.token))
    else:
        logger.error("Failed to connect to pyopencga")
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
    logger.info(process.stdout)
    # Check that the login worked
    if process.stderr != "":
        logger.error("Failed to connect to OpenCGA CLI")
        sys.exit(0)


def check_file_status(oc, metadata, file_name, attributes, check_attributes=False):
    """
    Perform file checks. First if file has already been uploaded (file name exists in files.search) and if so, check
    if the file has been already indexed and annotated.
    :param oc: openCGA client
    :param config: configuration dictionary
    :param file_name: name of the file that wants to be uploaded
    :param attributes: attributes dictionary (keys and values) to be checked
    :return: returns three booleans indicating whether the file has been uploaded, indexed and annotated
    """

    # Init check variables to None
    uploaded = None
    indexed = None
    annotated = None
    sample_ids = None

    # Search file in OpenCGA
    try:
        file_search = oc.files.search(study=metadata['study'], name=file_name)
    except Exception as e:
        logger.exception(msg=e)
        sys.exit(0)

    # File does not exist
    if file_search.get_num_results() == 0:
        uploaded = False
        logger.info("File {} does not exist in the OpenCGA study {}.".format(file_name, metadata['study']))
        # File exists and there's no more than one file with that name
    elif file_search.get_num_results() == 1:
        # Get statuses
        file_status = file_search.get_result(0)['internal']['status'][status_id]
        index_status = file_search.get_result(0)['internal']['index']['status'][status_id]
        annotation_status = 'NONE'
        # annotation_status = file_search.get_result(0)['internal']['annotationIndex']['status'][status_id]

        logger.info("Upload status: {}".format(file_status))
        logger.info("Index status: {}".format(index_status))
        # logger.info("Annotation status: {}".format(index_status))

        # Check upload status
        if file_status == "READY":
            uploaded = True
            logger.info("File {} already exists in the OpenCGA study {}. "
                      "Path to file: {}".format(file_name, metadata['study'], file_search.get_result(0)['path']))
            # Check attributes
            if check_attributes:
                for attr in attributes["attributes"]:
                    if attr in file_search.get_result(0)['attributes']:
                        if file_search.get_result(0)['attributes'][attr] == attributes["attributes"][attr]:
                            logger.info("Attribute {} matches the one in OpenCGA: {}".format(attr, attributes["attributes"][attr]))
                        else:
                            logger.warning("Attribute {} does not match the one stored in OpenCGA:\n- Provided: {}\n"
                                           "- Stored: {}".format(attr, attributes["attributes"][attr],
                                                                 file_search.get_result(0)['attributes'][attr]))
                    else:
                        logger.warning("Attribute {} is not included in openCGA".format(attr))
            # Get Sample(s) ID
            sample_ids = file_search.get_result(0)['sampleIds']
        else:
            uploaded = False
            # File exists but status is not READY - Needs to be uploaded again
            logger.info("File {} already exist in the OpenCGA study {} but status is {}. This file needs to be "
                        "uploaded again.".format(file_name, metadata['study'], file_status))

        # Check variant index status
        if index_status == "READY":
            indexed = True
            logger.info("File {} is indexed in the OpenCGA study {}.".format(file_name, metadata['study']))
        else:
            indexed = False

        # Check annotation index status
        if annotation_status == "READY":
            annotated = True
            logger.info("File {} is correctly annotated in the OpenCGA study {}.".format(file_name, metadata['study']))
        else:
            annotated = False

    # There is more than one file with this name in this study!
    else:
        logger.error("More than one file in OpenCGA with this name {} in study {}".format(file_name, metadata['study']))
        sys.exit(0)

    # TODO: Add extra checks for variant index and sample index
    # annotationIndex
    # secondaryIndex (no lo vamos a hacer)
    # sample: !!! get sample ID from file ^^^
    # index
    # genotypeIndex -- operation/variant/sample/index
    # annotationIndex

    return uploaded, indexed, annotated, sample_ids


def upload_file(opencga_cli, oc, metadata, file, attributes=dict(), file_path="data/"):
    """
    Uploads a file to the OpenCGA instance and stores it in the file path. It also updates the file to add the
    DNA nexus file ID as attribute
    :param opencga_cli: OpenCGA CLI
    :param oc: OpenCGA client
    :param metadata: metadata dictionary
    :param file: VCF file to upload
    :param attributes: attributes to be added to the file
    :param file_path: directory inside OpenCGA where the file should be stored (default: data/)
    """
    # Run upload using the bash CLI
    process = subprocess.Popen([opencga_cli, "files", "upload", "--input", file, "--study", metadata['study'],
                                "--catalog-path", file_path, "--parents"], stdout=PIPE, stderr=PIPE, text=True)
    process.wait()  # Wait until the execution is complete to continue with the program
    stdout, stderr = process.communicate()
    if stderr != "":
        logger.error(str(stderr))
        sys.exit(0)
    else:
        logger.info("File uploaded successfully. Path to file in OpenCGA catalog: {}".format(stdout.split('\t')[18]))
        logger.info("\n" + stdout)

    # Update file to contain the provided attributes
    try:
        oc.files.update(study=metadata['study'], files=os.path.basename(file), data=attributes)
    except Exception as e:
        logger.error("Failed to add the attributes to the file in OpenCGA")


def index_file(oc, metadata, file):
    """
    Indexes a VCF that has already been uploaded to OpenCGA
    :param oc: OpenCGA client
    :param metadata: metadata dictionary
    :param file: name of the VCF file already uploaded in OpenCGA
    """
    index_job = oc.variants.run_index(study=metadata['study'], data={"file": file})
    logger.info("Indexing file {} with job ID: {}".format(file, index_job.get_result(0)['id']))
    try:
        oc.wait_for_job(response=index_job.get_response(0))
    except ValueError as ve:
        logger.exception("OpenCGA failed to index the file. {}".format(ve))
        sys.exit(0)


def annotate_variants(oc, metadata):
    """
    Launches an OpenCGA job to force the annotation of any new variants added to the database.
    :param oc: OpenCGA client
    :param metadata: metadata dictionary
    """
    annotate_job = oc.variant_operations.index_variant_annotation(study=metadata['study'], data={})
    logger.info("Annotating new variants in study {} with job ID: {}".format(metadata['study'],
                                                                           annotate_job.get_result(0)['id']))
    try:
        oc.wait_for_job(response=annotate_job.get_response(0))
        oc.jobs.info(study=metadata['study'], jobs=annotate_job.get_result(0)['id'])
    except ValueError as ve:
        logger.exception("OpenCGA annotation job failed. {}".format(ve))
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
        logger.error("OpenCGA CLI not found.")
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

    # Format DNA Nexus file ID to attributes
    dnanexus_attributes = {"attributes": {
                                "DNAnexusFileId": args.dnanexus_fid}}

    # Check the status of the file and execute the necessary actions
    # Check upload status
    uploaded, indexed, annotated, sample_ids = check_file_status(oc=oc, metadata=metadata,
                                                                file_name=os.path.basename(args.vcf),
                                                                attributes=dnanexus_attributes, check_attributes=True)
    # if uploaded is not None:
    if not uploaded:
        upload_file(opencga_cli=opencga_cli, oc=oc, metadata=metadata, file=args.vcf, file_path=file_path,
                    attributes=dnanexus_attributes)

    # Check index status
    uploaded, indexed, annotated, sample_ids = check_file_status(oc=oc, metadata=metadata,
                                                                file_name=os.path.basename(args.vcf),
                                                                attributes=dnanexus_attributes, check_attributes=True)
    if indexed is not None:
        if not indexed:
            index_file(oc=oc, metadata=metadata, file=os.path.basename(args.vcf))

    # Check variant annotation status
    # NOTE: Cannot be done at the moment
    uploaded, indexed, annotated, sample_ids = check_file_status(oc=oc, metadata=metadata,
                                                                file_name=os.path.basename(args.vcf),
                                                                attributes=dnanexus_attributes, check_attributes=True)
    if annotated is not None:
        if not annotated:
            annotate_variants(oc=oc, metadata=metadata)

    # # Check again the status of the file
    # uploaded, indexed, annotated = check_file_status(oc=oc, config=config, file_name=os.path.basename(args.vcf))
    # if uploaded and indexed and annotated:
    #     logger.info("File {} has been successfully uploaded, indexed and annotated.")
    # else:
    #     logger.error("Something went wrong. Status of file {}:\n\t- uploaded: {}\n\t- indexed: {}\n\t- annotated: {}\n"
    #                "Please check the logs to identify the problem.".format(args.vcf, uploaded, indexed, annotated))
    oh.close()
    eh.close()
