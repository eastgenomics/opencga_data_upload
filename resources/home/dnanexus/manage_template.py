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

from resources.home.dnanexus.opencga_upload_and_index import read_metadata


def create_manifest(project_id, study_id, version, description=""):
    manifest = {
        "configuration": {
            "projectId": project_id,
            "version": version
        },
        "study": {
            "id": study_id,
            "description": description
        }
    }
    return manifest


def create_individual(ind_id, disorder_ids, **kwargs):
    individuals = [
        {
            "id": ind_id,
            "name": ind_id,
            "disorders": []
        }]
    for d in disorder_ids:
        dis_id = {'id': d}
        individuals[0]['disorders'].append(dis_id)
    if kwargs.get('sex') is not None:
        individuals[0]['sex'] = {
                "id": kwargs.get('sex'),
            }
    if kwargs.get('father_id') is not None:
        individuals[0]['father'] = {
            "id": kwargs.get('father_id'),
        }
    if kwargs.get('mother_id') is not None:
        individuals[0]['mother'] = {
            "id": kwargs.get('mother_id'),
        }
    if kwargs.get('karyotypicSex') is not None:
        individuals[0]['karyotypicSex'] = kwargs.get('karyotypicSex')
    if kwargs.get('lifeStatus') is not None:
        individuals[0]['lifeStatus'] = kwargs.get('lifeStatus')
    return individuals


def create_clinical(case_id, type, proband_id, proband_samples, disorder_id, **kwargs):
    clinical = [
        {
            "id": case_id,
            "type": type,
            "proband": [
                {
                    "id": proband_id,
                    "samples": []
                }],
            "family": {},
            "disorder": {
                "id": disorder_id
            }
        }]
    for ps in proband_samples:
        samples_id = {'id': ps}
        clinical[0]['proband'][0]['samples'].append(samples_id)
    if kwargs.get('family_id') is not None:
        clinical[0]['family']['id'] = kwargs.get('family_id')
    return clinical


def create_sample(sample_ids, individual_ids):
    if len(sample_ids) == len(individual_ids):
        samples = []
        for i in range(0, len(sample_ids)):
            sample_to_add = {
                'id': sample_ids[i],
                'individualId': individual_ids[i]
            }
            samples.append(sample_to_add)
        return samples
    else:
        logging.error("The number of sample IDs and individual IDs no not match")


def create_template(metadata_dict, output_directory):
    # create output dir if it does not exist
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    else:
        logging.warning(" The template output directory already exists, files will be overwritten")

    # create manifest
    manifest = create_manifest(project_id=metadata_dict['study'].split(':')[0],
                               study_id=metadata_dict['study'].split(':')[1],
                               version='2.2.0-SNAPSHOT')
    # create individual
    individual = create_individual(ind_id=metadata_dict['individual_id'],
                                   disorder_ids=metadata_dict['disorders'],
                                   sex=metadata_dict['sex'])
    # create sample
    sample = create_sample(sample_ids=[metadata_dict['sample_id']],
                           individual_ids=[metadata_dict['individual_id']])
    # create case
    clinical = create_clinical(case_id=metadata_dict['clinical_analysis_id'],
                               type=metadata_dict['case_type'],
                               proband_id=metadata_dict['individual_id'],
                               proband_samples=[metadata_dict['sample_id']],
                               disorder_id=metadata_dict['disorders'][0])

    # print objects to directory
    with open(file=output_directory + "/manifest.json", mode='w') as manifest_json:
        json.dump(obj=manifest, fp=manifest_json)
    with open(file=output_directory + "/individual.json", mode='w') as individual_json:
        json.dump(obj=individual, fp=individual_json)
    with open(file=output_directory + "/sample.json", mode='w') as sample_json:
        json.dump(obj=sample, fp=sample_json)
    with open(file=output_directory + "/clinical.json", mode='w') as clinical_json:
        json.dump(obj=clinical, fp=clinical_json)


if __name__ == '__main__':
    # Set the arguments of the command line
    parser = argparse.ArgumentParser(description='Create template to load metadata in catalog')
    parser.add_argument('--metadata', help='JSON file containing the metadata (minimum required information: "study")')
    parser.add_argument('--credentials', help='JSON file with credentials and host to access OpenCGA')
    parser.add_argument('--cli', help='Path to OpenCGA cli')
    args = parser.parse_args()

    # Read metadata file
    metadata = read_metadata(metadata_file=args.metadata)
    create_template(metadata_dict=metadata, output_directory="/home/mbleda/misc/eglh-myeloid/test_template")
