#!/usr/bin/env python3

# import required libraries
import datetime
from collections import defaultdict
from pathlib import Path
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

import concurrent
import dxpy

import concurrent
import dxpy


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


def link_metadata_vcfs(metadata_files, vcf_files):
    """ Create list of lists containing the metadata corresponding to vcfs

    Args:
        metadata_files (list): List of metadata files
        vcf_files (list): List of vcf files

    Returns:
        list: List of lists
    """

    data = []

    for file in metadata_files:
        file = Path(file)
        sample_name = file.name.split("_")[0]
        # look for the vcf files that have the name in them
        data.extend(
            [[Path(vcf), file] for vcf in vcf_files if sample_name in vcf]
        )

    return data


def link_vcfs_to_dnanexus_ids():
    """ Get the dnanexus ids from the job_input.json file in /home/dnanexus/
        and link those ids to the names of the vcf files

    Returns:
        dict: Dict linking vcf names to ids
    """

    with open('job_input.json') as fh:
        input_data = json.load(fh)

    # get just ids for the vcf input
    ids = [x['$dnanexus_link'] for x in input_data['vcfs']]
    ids_names = {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(ids)) as executor:
        # submit jobs mapping each id to describe call
        concurrent_jobs = {
            executor.submit(dxpy.describe, dnanexus_id): dnanexus_id
            for dnanexus_id in ids
        }

        for future in concurrent.futures.as_completed(concurrent_jobs):
            # access returned output as each is returned in any order
            try:
                response = future.result()
                ids_names[response['name']] = response['id']
            except Exception as exc:
                # catch any errors that might get raised during querying
                print(
                    f"Error getting data for {concurrent_jobs[future]}: {exc}"
                )

    return ids_names


def link_vcfs_to_dnanexus_ids():
    """ Get the dnanexus ids from the job_input.json file in /home/dnanexus/
        and link those ids to the names of the vcf files

    Returns:
        dict: Dict linking vcf names to ids
    """

    with open('job_input.json') as fh:
        input_data = json.load(fh)

    # get just ids for the vcf input
    ids = [x['$dnanexus_link'] for x in input_data['vcfs']]
    ids_names = {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(ids)) as executor:
        # submit jobs mapping each id to describe call
        concurrent_jobs = {
            executor.submit(dxpy.describe, dnanexus_id): dnanexus_id
            for dnanexus_id in ids
        }

        for future in concurrent.futures.as_completed(concurrent_jobs):
            # access returned output as each is returned in any order
            try:
                response = future.result()
                ids_names[response['name']] = response['id']
            except Exception as exc:
                # catch any errors that might get raised during querying
                print(
                    f"Error getting data for {concurrent_jobs[future]}: {exc}"
                )

    return ids_names


if __name__ == '__main__':
    # Set the arguments of the command line
    parser = argparse.ArgumentParser(description=' Load VCFs from DNANexus into OpenCGA')
    parser.add_argument('--project', help='OpenCGA Project where the file will be loaded')
    parser.add_argument('--study', help='OpenCGA Study where the file will be loaded')
    parser.add_argument('--metadata', nargs="+", help='Zip file(s) containing the metadata (minimum required information: "study")')
    parser.add_argument('--credentials', help='JSON file with credentials and host to access OpenCGA')
    parser.add_argument('--cli', help='Path to OpenCGA cli')
    # parser.add_argument('--cli21', help='Path to OpenCGA cli 2.1')
    parser.add_argument('--vcf', nargs="+", help='Input vcf(s) file')
    parser.add_argument('--somatic', help='Use the somatic flag if the sample to be loaded is somatic',
                        action='store_true')
    parser.add_argument('--multifile', help='Use the multifile flag if you expect to load multiple files from this '
                                            'sample', default=False, action='store_true')
    parser.add_argument('--dnanexus_project', help='DNAnexus project ID')
                                            'sample', default=False, action='store_true')
    parser.add_argument('--dnanexus_project', help='DNAnexus project ID')
    args = parser.parse_args()

    # Check the location of the OpenCGA CLI
    if not os.path.isfile(args.cli):
        logger.error("OpenCGA CLI not found.")
        sys.exit(1)
    opencga_cli = args.cli

    # Check if metadata has been provided
    project = args.project
    study = args.study

    vcf_data = defaultdict(lambda: defaultdict(str))

    if args.metadata:
        logger.info("Metadata files provided")
        # Link vcfs and metadata together
        vcf_with_metadata = link_metadata_vcfs(args.metadata, args.vcf)

        for vcf_file, metadata_file in vcf_with_metadata:
            manifest, samples, individuals, clinical = read_metadata(metadata_file=metadata_file, logger=logger)
            vcf_data[vcf_file]["project"] = manifest['configuration']['projectId']
            vcf_data[vcf_file]["study"] = manifest['study']['id']
            vcf_data[vcf_file]["study_fqn"] = f"{manifest['configuration']['projectId']}:{manifest['study']['id']}"
            vcf_data[vcf_file]["samples"] = samples
            vcf_data[vcf_file]["individuals"] = individuals
            vcf_data[vcf_file]["clinical"] = clinical
            # Get case priority. If case priority is URGENT, jobs will not be delayed
            delay = True
            priority = clinical[0]['priority']['id']
            if priority in no_delay_priority:
                delay = False

            vcf_data[vcf_file]["delay"] = delay

            multi_file = args.multifile

            if clinical[0]['type'] == 'CANCER':
                multi_file = True

            vcf_data[vcf_file]["multi_file"] = multi_file

        for vcf_file, metadata_file in vcf_with_metadata:
            manifest, samples, individuals, clinical = read_metadata(metadata_file=metadata_file, logger=logger)
            vcf_data[vcf_file]["project"] = manifest['configuration']['projectId']
            vcf_data[vcf_file]["study"] = manifest['study']['id']
            vcf_data[vcf_file]["study_fqn"] = f"{manifest['configuration']['projectId']}:{manifest['study']['id']}"
            vcf_data[vcf_file]["samples"] = samples
            vcf_data[vcf_file]["individuals"] = individuals
            vcf_data[vcf_file]["clinical"] = clinical
            # Get case priority. If case priority is URGENT, jobs will not be delayed
            delay = True
            priority = clinical[0]['priority']['id']
            if priority in no_delay_priority:
                delay = False

            vcf_data[vcf_file]["delay"] = delay

            multi_file = args.multifile

            if clinical[0]['type'] == 'CANCER':
                multi_file = True

            vcf_data[vcf_file]["multi_file"] = multi_file
    else:
        logger.info("No metadata has been provided, VCF will not be associated to any individuals or cases")
        if args.project is not None and args.study is not None:
            for vcf_file in args.vcf:
                vcf_data[vcf_file]["project"] = project
                vcf_data[vcf_file]["study"] = study
                vcf_data[vcf_file]["study_fqn"] = f"{project}:{study}"
                vcf_data[vcf_file]["samples"] = None
                vcf_data[vcf_file]["individuals"] = None
                vcf_data[vcf_file]["clinical"] = None

            for vcf_file in args.vcf:
                vcf_data[vcf_file]["project"] = project
                vcf_data[vcf_file]["study"] = study
                vcf_data[vcf_file]["study_fqn"] = f"{project}:{study}"
                vcf_data[vcf_file]["samples"] = None
                vcf_data[vcf_file]["individuals"] = None
                vcf_data[vcf_file]["clinical"] = None

            logger.info("Data will be loaded in study: {}:{}".format(project, study))
        else:
            logger.error("No project or study provided. Please provide a metadata file or specify the project and "
                         "study where data needs to be loaded.")
            sys.exit(1)

    # Read credentials file
    credentials = get_credentials(credentials_file=args.credentials)

    # Login OpenCGA CLI
    connect_cli(credentials=credentials, opencga_cli=opencga_cli, logger=logger)
    # connect_cli(credentials=credentials, opencga_cli=opencga_cli21, logger=logger)

    # Create pyopencga client
    oc = connect_pyopencga(credentials=credentials, logger=logger)

    # Get today's date to store the file in a directory named as "YearMonth" (e.g. 202112 = December 2021)
    date_folder = datetime.date.today().strftime("%Y%m")
    file_path = "data/" + date_folder

    # Check study to define index type
    somatic = args.somatic

    vcf2ids = link_vcfs_to_dnanexus_ids()
    vcf2ids = link_vcfs_to_dnanexus_ids()

    # go through each vcf and upload and index them
    for vcf in vcf_data:
        proj_study = vcf_data[vcf]["study_fqn"]
        multi_file = vcf_data[vcf]["multi_file"]
        # Format DNAnexus file ID to attributes
        file_data = {}
        file_data["attributes"] = {
            "DNAnexusFileId": vcf2ids[vcf.name]
        }

        # define software
        if 'tnhaplotyper2' in os.path.basename(vcf):
            file_data['software'] = {'name': 'TNhaplotyper2'}
        if '.flagged.' in os.path.basename(vcf):
            file_data['software'] = {'name': 'Pindel'}
        if '.SV.' in os.path.basename(vcf):
            file_data['software'] = {'name': 'Manta'}
        if os.path.basename(vcf).startswith('EH_'):
            file_data['software'] = {'name': 'ExpansionHunter'}
        # define software
        if 'tnhaplotyper2' in os.path.basename(vcf):
            file_data['software'] = {'name': 'TNhaplotyper2'}
        if '.flagged.' in os.path.basename(vcf):
            file_data['software'] = {'name': 'Pindel'}
        if '.SV.' in os.path.basename(vcf):
            file_data['software'] = {'name': 'Manta'}
        if os.path.basename(vcf).startswith('EH_'):
            file_data['software'] = {'name': 'ExpansionHunter'}

        # Check the status of the file and execute the necessary actions
        uploaded, indexed, annotated, sample_index, existing_file_path, sample_ids = check_file_status(oc=oc,
                                                                                            study=proj_study,
                                                                                            file_name=os.path.basename(vcf),
                                                                                            file_info=file_data,
                                                                                            logger=logger, check_attributes=True)

        # UPLOAD
        if uploaded:
            logger.info("File {} already exists in the OpenCGA study {}. "
                        "Path to file: {}".format(os.path.basename(vcf), proj_study, existing_file_path))
        else:
            logger.info("Uploading file {} into study {}...".format(os.path.basename(vcf), proj_study))
            upload_file(opencga_cli=opencga_cli, oc=oc, study=proj_study, file=vcf, file_path=file_path,
                        file_info=file_data, logger=logger)

        # INDEXING
        if indexed:
            logger.info("File {} is indexed in the OpenCGA study {}.".format(os.path.basename(vcf), proj_study))
        else:
            logger.info("Indexing file {} into study {}...".format(os.path.basename(vcf), proj_study))
            index_file(oc=oc, study=proj_study, file=os.path.basename(vcf), logger=logger,
                    somatic=somatic, multifile=multi_file)

    study_fqn = f"{project}:{study}"

    # Launch variant stats index
    logger.info("Launching variant stats...")
    vsi_job = variant_stats_index(oc=oc, study=proj_study, cohort='ALL', logger=logger)
    # TODO: Check status of this job at the end

    # ANNOTATION
    if annotated:
        logger.info("File {} is already annotated in the OpenCGA study {}.".format(os.path.basename(vcf_file),
                                                                                study_fqn))
    else:
        logger.info("Annotating file {} into study {}...".format(os.path.basename(vcf_file), study_fqn))
        annotate_variants(oc=oc, project=project, study=study, logger=logger)
    # ANNOTATION
    if annotated:
        logger.info("File {} is already annotated in the OpenCGA study {}.".format(os.path.basename(vcf_file),
                                                                                study_fqn))
    else:
        logger.info("Annotating file {} into study {}...".format(os.path.basename(vcf_file), study_fqn))
        annotate_variants(oc=oc, project=project, study=study, logger=logger)

    # Launch sample stats index
    logger.info("Launching sample stats...")
    # svs_job = sample_variant_stats(oc=oc, study=study_fqn, sample_ids=sample_ids, logger=logger)
    # TODO: Check status of this job at the end
    # Launch sample stats index
    logger.info("Launching sample stats...")
    # svs_job = sample_variant_stats(oc=oc, study=study_fqn, sample_ids=sample_ids, logger=logger)
    # TODO: Check status of this job at the end

    # SECONDARY ANNOTATION INDEX
    secondary_annotation_index(oc=oc, study=study_fqn, logger=logger)
    # SECONDARY ANNOTATION INDEX
    secondary_annotation_index(oc=oc, study=study_fqn, logger=logger)

    logger.info("Loading metadata...")
    # LOAD TEMPLATE
    # load_template(oc=oc, study=manifest['study']['id'], template=args.metadata,
    #               logger=logger)
    logger.info("Loading metadata...")
    # LOAD TEMPLATE
    # load_template(oc=oc, study=manifest['study']['id'], template=args.metadata,
    #               logger=logger)

    for vcf in vcf_data:
        individuals = vcf_data[vcf]["individuals"]
        samples = vcf_data[vcf]["samples"]
        clinical = vcf_data[vcf]["individuals"]

        if individuals and samples and clinical:
            study_fqn = vcf_data[vcf]["study_fqn"]
    for vcf in vcf_data:
        individuals = vcf_data[vcf]["individuals"]
        samples = vcf_data[vcf]["samples"]
        clinical = vcf_data[vcf]["individuals"]

        if individuals and samples and clinical:
            study_fqn = vcf_data[vcf]["study_fqn"]
            # CREATE IND
            # Get sample ID
            sampleIds = oc.files.info(study=study_fqn, files=os.path.basename(vcf), include="sampleIds").get_result(0)['sampleIds']
            sampleIds = oc.files.info(study=study_fqn, files=os.path.basename(vcf), include="sampleIds").get_result(0)['sampleIds']
            if len(sampleIds) >= 1 and 'TA2_S59_L008_tumor' in sampleIds:
                sampleIds.remove('TA2_S59_L008_tumor')
            if len(sampleIds) < 1:
                logger.error("Unexpected number of samples in the VCF")
                sys.exit(1)

            # Define individual
            individual_id = individuals['id']
            ind_data = {
                'id': individual_id,
                'name': individual_id,
                'disorders': [{
                    'id': 'HaemOnc'
                }],
                'sex': individuals['sex']
            }
            # Check if individual exists
            logger.info("Checking if individual exists...")
            check_individual = oc.individuals.search(study=study_fqn, id=individual_id).get_num_results()
            if check_individual == 0:
                logger.info("Creating new individual {}...".format(individual_id))
                oc.individuals.create(study=study_fqn, samples='{}'.format(",".join(sampleIds)), data=ind_data)
            elif check_individual > 0:
                logger.info("Individual {} already exists in the database. No action needed.".format(individual_id))
                # oc.individuals.update(study=manifest['study']['id'], individuals=individual_id, data=ind_data,
                #                       samples='{}'.format(",".join(sampleIds)), samples_action='ADD')
            # associate sample and individual
            for sampleID in sampleIds:
                oc.samples.update(study=study_fqn, samples=sampleID, data={'individualId': individuals['id'],
                                                                        'somatic': somatic})

            # CREATE CASE
            logger.info("Checking if clinical case exists...")
            clinical_case = {
                'id': clinical[0]['id'],
                'type': clinical[0]['type'],
                'proband': {
                    'id': individuals['id'],
                    'samples': [{
                        'id': samples[0]['id'],
                    }]
                },
                'disorder': clinical[0]['disorder'],
                'panels': [{'id': 'myeloid_genes'}],
                'priority': clinical[0]['priority'],
                'comments': [{
                    'message': 'Case created automatically',
                    'tags': ['auto', 'validation']
                }],
                'status': {'id': 'READY_FOR_INTERPRETATION'}
            }
            check_case = oc.clinical.search(study=study_fqn, id=clinical_case["id"]).get_num_results()
            if check_case == 0:
                logger.info("Creating new clinical case {}...".format(clinical_case["id"]))
                oc.clinical.create(data=clinical_case, study=study_fqn, createDefaultInterpretation=True)
            elif check_case == 1:
                logger.info("Case {} already exists in the database.".format(clinical_case["id"]))

            # SECONDARY SAMPLE INDEX
            secondary_sample_index(oc=oc, study=study_fqn, sample=sampleIds[0], logger=logger)

            # Check again the status of the file
            uploaded, indexed, annotated, sample_index, existing_file_path, sample_ids = check_file_status(oc=oc,
                                                                                                study=study_fqn,
                                                                                                file_name=os.path.basename(vcf),
                                                                                                file_name=os.path.basename(vcf),
                                                                                                file_info=file_data,
                                                                                                logger=logger, check_attributes=True)

    # close loggers
    oh.close()
    eh.close()
