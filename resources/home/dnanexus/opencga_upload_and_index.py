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

no_delay_priority = ['URGENT']


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
    manifest['study']['id'] = 'validation:myeloid'

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
    if priority in no_delay_priority:
        delay = False

    # Check study to define index type
    somatic = False
    multi_file = False
    if clinical[0]['type'] == 'CANCER':
        multi_file = True
        normal = None
        tumor = None
        # define software
        if 'tnhaplotyper2' in os.path.basename(args.vcf):
            file_data = {'software': {'name': 'TNhaplotyper2'}}
            somatic = True
        else:
            file_data = {'software': {'name': 'Pindel'}}
        # # Extract germline and tumour sample names
        # with open(args.vcf, 'r') as cancer_vcf:
        #     for line in cancer_vcf:
        #         if line.startswith('##SAMPLE=<ID=NORMAL'):
        #             match_name = re.search(".+SampleName=(.+)>", line)
        #             normal = match_name.group(1)
        #         if line.startswith('##SAMPLE=<ID=TUMOUR'):
        #             match_name = re.search(".+SampleName=(.+)>", line)
        #             tumor = match_name.group(1)

    # Check the status of the file and execute the necessary actions
    uploaded, indexed, annotated, sample_index, existing_file_path, sample_ids = check_file_status(oc=oc,
                                                                                          study=manifest['study']['id'],
                                                                                          file_name=os.path.basename(args.vcf),
                                                                                          attributes=dnanexus_attributes,
                                                                                          logger=logger, check_attributes=True)

    # UPLOAD
    if uploaded:
        logger.info("File {} already exists in the OpenCGA study {}. "
                    "Path to file: {}".format(os.path.basename(args.vcf), manifest['study']['id'], existing_file_path))
    else:
        logger.info("Uploading file {} into study {}...".format(os.path.basename(args.vcf), manifest['study']['id']))
        upload_file(opencga_cli=opencga_cli, oc=oc, study=manifest['study']['id'], file=args.vcf, file_path=file_path,
                    attributes=dnanexus_attributes, logger=logger)

    # INDEXING
    if indexed:
        logger.info("File {} is indexed in the OpenCGA study {}.".format(os.path.basename(args.vcf),
                                                                         manifest['study']['id']))
    else:
        logger.info("Indexing file {} into study {}...".format(os.path.basename(args.vcf), manifest['study']['id']))
        index_file(oc=oc, study=manifest['study']['id'], file=os.path.basename(args.vcf), logger=logger,
                   somatic=somatic, multifile=multi_file)

    # Launch variant stats index
    logger.info("Launching variant stats...")
    vsi_job = variant_stats_index(oc=oc, study=manifest['study']['id'], cohort='ALL', logger=logger)
    # # TODO: Check status of this job at the end

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
    # # TODO: Check status of this job at the end

    # SECONDARY ANNOTATION INDEX
    if secondary_indexed:
        logger.info("File {} is already indexed in Solr in the OpenCGA study {}.".format(os.path.basename(args.vcf),
                                                                                         manifest['study']['id']))
    else:
        logger.info("Updating Solr index in study {}...".format(manifest['study']['id']))
        secondary_index(oc=oc, study=manifest['study']['id'], logger=logger)

    # LOAD TEMPLATE
    load_template(c=oc, study=manifest['study']['id'], logger=logger)

### OLD ####
    # # CREATE IND
    # # Get sample ID
    # sampleIds = oc.files.info(study=manifest['study']['id'], files=os.path.basename(args.vcf), include="sampleIds").get_result(0)['sampleIds']
    # if len(sampleIds) < 1:
    #     logger.error("Unexpected number of samples in the VCF")
    #     sys.exit(1)
    # else:
    #     for sampleID in sampleIds:
    #         # Update sample information
    #         if sampleID == normal:
    #             somatic = False
    #         elif sampleID == tumor:
    #             somatic = True
    #         oc.samples.update(study=manifest['study']['id'], samples=sampleID, data={'somatic': somatic})
    #
    #     # Define individual
    #     individual_id = samples[0]['id']
    #     ind_data = {
    #         'id': individual_id,
    #         'name': individual_id,
    #         'disorders': [{
    #             'id': 'HaemOnc'
    #         }],
    #         'sex': individuals['sex']
    #     }
    #     # Check if individual exists
    #     logger.info("Checking if individual exists...")
    #     check_individual = oc.individuals.search(study=manifest['study']['id'], id=individual_id).get_num_results()
    #     if check_individual == 0:
    #         logger.info("Creating new individual {}...".format(individual_id))
    #         oc.individuals.create(study=manifest['study']['id'], samples='{}'.format(tumor), data=ind_data)
    #     elif check_individual > 0:
    #         logger.info("Individual {} already exists in the database. No action needed.".format(individual_id))
    #         # oc.individuals.update(study=manifest['study']['id'], individuals=individual_id, data=ind_data,
    #         #                       samples='{}'.format(",".join(sampleIds)), samples_action='ADD')
    #
    # # CREATE CASE
    # logger.info("Checking if clinical case exists...")
    # clinical_case = {
    #     'id': clinical[0]['id'],
    #     'type': clinical[0]['type'],
    #     'proband': {
    #         'id': samples[0]['id'],
    #         'samples': [{
    #             'id': samples[0]['individualId'],
    #         }]
    #     },
    #     'disorder': clinical[0]['disorder'],
    #     'panels': [{'id': 'myeloid_genes'}],
    #     'priority': clinical[0]['priority'],
    #     'comments': [{
    #         'message': 'Case created automatically',
    #         'tags': ['auto', 'validation']
    #     }],
    #     'status': {'id': 'READY_FOR_INTERPRETATION'}
    # }
    # check_case = oc.clinical.search(study=manifest['study']['id'], id=clinical_case["id"]).get_num_results()
    # if check_case == 0:
    #     logger.info("Creating new clinical case {}...".format(clinical_case["id"]))
    #     oc.clinical.create(data=clinical_case, study=manifest['study']['id'], createDefaultInterpretation=True)
    # elif check_case == 1:
    #     logger.info("Case {} already exists in the database.".format(clinical_case["id"]))

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
