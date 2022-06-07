#!/usr/bin/env python3

# import required libraries
import datetime
import gzip
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
import re


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


if __name__ == '__main__':
    # Set the arguments of the command line
    parser = argparse.ArgumentParser(description='Load VCFs from DNANexus into OpenCGA')
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

    # Read credentials file
    credentials = get_credentials(credentials_file=args.credentials)

    # Login OpenCGA CLI
    connect_cli(credentials=credentials, opencga_cli=opencga_cli, logger=logger)

    # Create pyopencga client
    oc = connect_pyopencga(credentials=credentials, logger=logger)

    # Get today's date to store the file in a directory named as "YearMonth" (e.g. 202112 = December 2021)
    #date_folder = datetime.date.today().strftime("%Y%m")
    date_folder = "202205"
    file_path = "data/" + date_folder

    # Format DNA Nexus file ID to attributes
    dnanexus_attributes = {"attributes": {
                                "DNAnexusFileId": args.dnanexus_fid}}

    # Define index type
    somatic = True
    multi_file = True
    study_id = 'tso500'

    # Check variant type
    structural = False
    if 'SV' in os.path.basename(args.vcf):
        structural = True
        file_data = {'software': {'name': 'manta'}}
    else:
        file_data = {'software': {'name': 'tnhaplotyper2'}}

    # UPLOAD
    logger.info("Uploading file {} into study {}...".format(os.path.basename(args.vcf), study_id))
    upload_file(opencga_cli=opencga_cli, oc=oc, study=study_id, file=args.vcf, file_path=file_path,
                attributes=dnanexus_attributes, logger=logger)

    # INDEXING
    logger.info("Indexing file {} into study {}...".format(os.path.basename(args.vcf), study_id))
    index_file(oc=oc, study=study_id, file=os.path.basename(args.vcf), logger=logger,
               somatic=somatic, multifile=multi_file)

    # UPDATE FILE
    logger.info("Updating file information...")
    oc.files.update(study=study_id, files=os.path.basename(args.vcf), data=file_data)

    # UPDATE SAMPLE
    # parse VCF
    # with gzip.open(args.vcf, 'r') as snv_vcf:
    #     for line in snv_vcf:
    #         line_dec = line.decode('UTF-8')
    #         if line_dec.startswith('##cmdline='):
    #             cmd_fields = line_dec.split(' ')
    #             normal = None
    #             tumor = None
    #             for f in cmd_fields:
    #                 if f.startswith('--normalBam'):
    #                     n = re.search("LP[0-9]+-DNA_[A-Z][0-9]+", f)
    #                     normal = n.group()
    #                 if f.startswith('--tumorBam'):
    #                     t = re.search("LP[0-9]+-DNA_[A-Z][0-9]+", f)
    #                     tumor = t.group()
    # logger.info("Updating sample information:\n- Germline: {}\n- Tumor: {}".format(normal, tumor))
    # if tumor is not None:
    #     oc.samples.update(study=study_id, samples=tumor, data={'somatic': True})

    # CREATE IND
    # Get sample ID
    sampleIds = oc.files.info(study=study_id, files=os.path.basename(args.vcf), include="sampleIds").get_result(0)['sampleIds']
    if len(sampleIds) != 1:
        logger.error("Unexpected number of samples in the VCF")
        sys.exit(1)
    else:
        sampleID = sampleIds[0]
        logger.info("Checking if individual exists...")
        individual_id = sampleID
        ind_data = {
            'id': individual_id,
        }
        oc.samples.update(study=study_id, samples=sampleID, data={'somatic': True})
        check_individual = oc.individuals.search(study=study_id, id=individual_id).get_num_results()
        if check_individual == 0:
            logger.info("Creating new individual {}...".format(individual_id))
            oc.individuals.create(study=study_id, samples='{}'.format(sampleID), data=ind_data)
        elif check_individual == 1:
            logger.info("Individual {} already exists in the database.".format(individual_id))

    # CREATE CASE
    logger.info("Checking if clinical case exists...")
    clinical_case = {
        'id': 'C-' + sampleID,
        'type': 'CANCER',
        'proband': {
            'id': sampleID,
        },
        'analyst': {'id': 'emee-glh'},
        'comments': [{
            'message': 'Case created automatically',
            'tags': ['auto']
        }],
        'status': {'id': 'READY_FOR_INTERPRETATION'}
    }
    check_case = oc.clinical.search(study=study_id, id=clinical_case["id"]).get_num_results()
    if check_case == 0:
        logger.info("Creating new clinical case {}...".format(clinical_case["id"]))
        oc.clinical.create(clinical_case, study=study_id, createDefaultInterpretation=True)
    elif check_case == 1:
        logger.info("Case {} already exists in the database.".format(clinical_case["id"]))

    # close loggers
    oh.close()
    eh.close()
