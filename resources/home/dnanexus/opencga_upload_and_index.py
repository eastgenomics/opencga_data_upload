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
from opencga_functions import *


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
formatter = logging.Formatter('%(asctime)s - %(module)s - %(levelname)s: %(message)s')
eh.setFormatter(formatter)
oh.setFormatter(formatter)
console.setFormatter(formatter)
# add the handlers to logger
logger.addHandler(eh)
logger.addHandler(oh)
logger.addHandler(console)


def build_variant_sample_index(oc, metadata, sample_ids):
    """
    Build and annotate the sample index for the selected list of samples
    :param oc: OpenCGA client
    :param metadata: metadata dictionary
    :param sample_ids: list of sample IDs
    """
    variant_sample_index_job = oc.variant_operations.index_sample_genotype(study=metadata['study'], data={'sample': sample_ids})
    logger.info("Building variant sample indices for sample(s) {} with job ID: {}".format(', '.join(sample_ids),
                                                                        variant_sample_index_job.get_result(0)['id']))
    try:
        oc.wait_for_job(response=variant_sample_index_job.get_response(0))
        status = oc.jobs.info(study=metadata['study'], jobs=variant_sample_index_job.get_result(0)['id'])
        if status.get_result(0)['execution']['status']['name'] == 'DONE':
            logger.info(
                "OpenCGA job variant sample index completed successfully for sample(s).".format(', '.join(sample_ids)))
        else:
            logger.info(
                "OpenCGA job variant sample index failed with status {}.".format(
                    status.get_result(0)['execution']['status']['name']))
    except ValueError as ve:
        logger.exception("OpenCGA job svariant sample index failed. {}".format(ve))
        sys.exit(0)


if __name__ == '__main__':
    # Set the arguments of the command line
    parser = argparse.ArgumentParser(description=' Index VCFs from DNANexus into OpenCGA')
    parser.add_argument('--metadata', help='Zip file containing the metadata (minimum required information: "study")')
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
    manifest, samples, individuals, clinical = read_metadata(metadata_file=args.metadata, logger=logger)

    # Read credentials file
    credentials = get_credentials(credentials_file=args.credentials)

    # Login OpenCGA CLI
    connect_cli(credentials=credentials, opencga_cli=opencga_cli, logger=logger)

    # Create pyopencga client
    oc = connect_pyopencga(credentials=credentials, logger=logger)

    # Get today's date to store the file in a directory named as "YearMonth" (e.g. 202112 = December 2021)
    date_folder = datetime.date.today().strftime("%Y%m")
    file_path = "data/" + date_folder

    # Format DNA Nexus file ID to attributes
    dnanexus_attributes = {"attributes": {
                                "DNAnexusFileId": args.dnanexus_fid}}

    # Get case priority. If case priority is URGENT, jobs will not be delayed
    delay = True
    priority = clinical[0]['priority']['id']
    if priority == 'URGENT':
        delay = False

    # Check study to define index type
    somatic = False
    multi_file = False
    if clinical['type'] == 'CANCER':
        somatic = True
        multi_file = True

    # Check the status of the file and execute the necessary actions
    uploaded, indexed, annotated, secondary_indexed, file_path, sample_ids = check_file_status(oc=oc,
                                                                 study=manifest['study']['id'],
                                                                 file_name=os.path.basename(args.vcf),
                                                                 attributes=dnanexus_attributes,
                                                                 logger=logger, check_attributes=True)

    # UPLOAD
    if uploaded:
        logger.info("File {} already exists in the OpenCGA study {}. "
                    "Path to file: {}".format(os.path.basename(args.vcf), manifest['study']['id'], file_path))
    else:
        logger.info("Uploading file {} into study {}...".format(os.path.basename(args.vcf), manifest['study']['id']))
        # upload_file(opencga_cli=opencga_cli, oc=oc, study=manifest['study']['id'], file=args.vcf, file_path=file_path,
        #             attributes=dnanexus_attributes, logger=logger)

    # INDEXING
    if indexed:
        logger.info("File {} is indexed in the OpenCGA study {}.".format(os.path.basename(args.vcf),
                                                                         manifest['study']['id']))
    else:
        logger.info("Indexing file {} into study {}...".format(os.path.basename(args.vcf), manifest['study']['id']))
        # index_file(oc=oc, study=manifest['study']['id'], file=os.path.basename(args.vcf), logger=logger,
        #            somatic=somatic, multifile=multi_file)

    # Launch variant stats index
    logger.info("Launching variant stats...")
    vsi_job = variant_stats_index(oc=oc, study=manifest['study']['id'], cohort='ALL', logger=logger)
    # TODO: Check status of this job at the end

    # ANNOTATION
    if annotated:
        logger.info("File {} is already annotated in the OpenCGA study {}.".format(os.path.basename(args.vcf),
                                                                                   manifest['study']['id']))
    else:
        logger.info("Annotating file {} into study {}...".format(os.path.basename(args.vcf), manifest['study']['id']))
        annotate_variants(oc=oc, study=manifest['study']['id'], logger=logger, delay=delay)

    # Run sample variant stats
    logger.info("Launching variant stats...")
    svs_job = sample_variant_stats(oc=oc, study=manifest['study']['id'], sample_ids=sample_ids, logger=logger)
    # TODO: Check status of this job at the end

    # SECONDARY ANNOTATION INDEX
    if secondary_indexed:
        logger.info("File {} is already indexed in Solr in the OpenCGA study {}.".format(os.path.basename(args.vcf),
                                                                                         manifest['study']['id']))
    else:
        logger.info("Updating Solr index in study {}...".format(manifest['study']['id']))
        secondary_index(oc=oc, study=manifest['study']['id'], logger=logger)

    # LOAD TEMPLATE
    load_template()

    # Run variant sample index
    #build_variant_sample_index(oc=oc, metadata=metadata, sample_ids=sample_ids)

    # # Check again the status of the file
    # uploaded, indexed, annotated = check_file_status(oc=oc, config=config, file_name=os.path.basename(args.vcf))
    # if uploaded and indexed and annotated:
    #     logger.info("File {} has been successfully uploaded, indexed and annotated.")
    # else:
    #     logger.error("Something went wrong. Status of file {}:\n\t- uploaded: {}\n\t- indexed: {}\n\t- annotated: {}\n"
    #                "Please check the logs to identify the problem.".format(args.vcf, uploaded, indexed, annotated))

    # close loggers
    oh.close()
    eh.close()
