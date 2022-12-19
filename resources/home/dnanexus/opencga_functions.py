#!/usr/bin/env python3
import os
import sys
import json
import zipfile
import yaml
from pyopencga.opencga_client import OpencgaClient
from pyopencga.opencga_config import ClientConfiguration
import subprocess
from subprocess import PIPE

# Define status id
status_id = "name"  # Used to be "name" in v2.1, but we are moving to using "id" since v2.2


def get_credentials(credentials_file):
    """
    Get the credentials from a JSON file to log into the OpenCGA instance
    :param credentials_file: JSON file containing the credentials and the host to connect to OpenCGA
    :return: dictionary with credentials and host
    """
    credentials_dict = json.load(open(credentials_file, 'r'))
    return credentials_dict


def read_metadata(metadata_file, logger):
    """
    Load the information in the metadata file. The metadata file should be a zipped file containing four YAML files
    (manifest.yaml, samples.yaml, individuals.yaml and clinical.yaml) with the necessary information to create the
    samples, individuals and clinical cases associated to a VCF. These files must also follow the OpenCGA data models
    :param metadata_file: Zip file containing the metadata necessary to create samples, individuals and cases
    :param logger: logger object to generate logs
    :return: dictionary with metadata params
    """
    manifest = samples = individuals = clinical = None
    with zipfile.ZipFile(metadata_file, "r") as meta_zip:
        try:
            manifest = yaml.safe_load(meta_zip.open('manifest.yaml'))
            samples = yaml.safe_load(meta_zip.open('samples.yaml'))
            individuals = yaml.safe_load(meta_zip.open('individuals.yaml'))
            clinical = yaml.safe_load(meta_zip.open('clinical.yaml'))
        except ValueError as ve:
            logger.error("Failed to read metadata, please, check your zip file.")
            sys.exit(1)
    return manifest, samples, individuals, clinical


def connect_pyopencga(credentials, logger):
    """
    Connect to pyopencga
    :param credentials: dictionary of credentials and host.
    :param logger: logger object to generate logs
    """
    opencga_config_dict = {'rest': {'host': credentials['host']}}
    opencga_config = ClientConfiguration(opencga_config_dict)
    oc = OpencgaClient(opencga_config, auto_refresh=True)
    oc.login(user=credentials['user'],
             password=credentials['password'])
    if oc.token is not None:
        logger.info("Successfully connected to pyopencga")
    else:
        logger.error("Failed to connect to pyopencga")
        sys.exit(1)
    return oc


def connect_cli(credentials, opencga_cli, logger):
    """
    Connect OpenCGA CLI to instance
    :param opencga_cli: OpenCGA CLI
    :param credentials: dictionary of credentials and host
    :param logger: logger object to generate logs
    """
    # Launch login on the CLI
    process = subprocess.run([opencga_cli, "users", "login", "-u", credentials['user'], "-p"],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                             input=credentials['password'])
    logger.info(process.stdout)
    # Check that the login worked
    if "ERROR" in process.stderr:
        logger.error("Failed to connect to OpenCGA CLI")
        logger.error(process.stderr)
        sys.exit(1)


def check_file_status(oc, study, file_name, file_info, logger, check_attributes=False):
    """
    Perform file checks. First if file has already been uploaded (file name exists in files.search) and if so, check
    if the file has been already indexed and annotated.
    :param oc: openCGA client
    :param study: study ID
    :param file_name: name of the file to be uploaded
    :param file_info: attributes dictionary (keys and values) to be checked
    :return: returns three booleans indicating whether the file has been uploaded, indexed and annotated
    :param logger: logger object to generate logs
    """

    # Init check variables to None
    uploaded = None
    indexed = None
    annotated = None
    sample_index = None
    sample_ids = None
    file_path = None

    # Search file in OpenCGA
    try:
        file_search = oc.files.search(study=study, name=file_name)
    except Exception as e:
        logger.exception(msg=e)
        sys.exit(1)

    # File does not exist
    if file_search.get_num_results() == 0:
        uploaded = False
        logger.info("File {} does not exist in the OpenCGA study {}.".format(file_name, study))
        # File exists and there's no more than one file with that name
    elif file_search.get_num_results() == 1:
        # Get statuses
        file_status = file_search.get_result(0)['internal']['status'][status_id]
        index_status = file_search.get_result(0)['internal']['variant']['index']['status'][status_id]
        annotation_status = file_search.get_result(0)['internal']['variant']['annotationIndex']['status'][status_id]
        secondary_sample_index_status = file_search.get_result(0)['internal']['variant']['secondaryIndex']['status'][status_id]

        logger.info("File status: {}".format(file_status))
        logger.info("Index status: {}".format(index_status))
        logger.info("Annotation status: {}".format(annotation_status))
        logger.info("Secondary sample index status: {}".format(secondary_sample_index_status))

        # Check upload status
        if file_status == "READY":
            uploaded = True
            file_path = file_search.get_result(0)['path']
            sample_ids = file_search.get_result(0)['sampleIds']
            # logger.info("File {} already exists in the OpenCGA study {}. "
            #             "Path to file: {}".format(file_name, metadata['study'], file_search.get_result(0)['path']))
            # Check attributes
            if check_attributes:
                for attr in file_info["attributes"]:
                    if attr in file_search.get_result(0)['attributes']:
                        if file_search.get_result(0)['attributes'][attr] == file_info["attributes"][attr]:
                            logger.info("Attribute {} matches the one in OpenCGA: {}".format(attr, file_info["attributes"][attr]))
                        else:
                            logger.warning("Attribute {} does not match the one stored in OpenCGA:\n- Provided: {}\n"
                                           "- Stored: {}".format(attr, file_info["attributes"][attr],
                                                                 file_search.get_result(0)['attributes'][attr]))
                    else:
                        logger.warning("Attribute {} is not included in openCGA".format(attr))
        else:
            uploaded = False
            # File exists but status is not READY - Needs to be uploaded again
            # logger.info("File {} already exist in the OpenCGA study {} but status is {}. This file needs to be "
            #             "uploaded again.".format(file_name, metadata['study'], file_status))

        # Check variant index status
        if index_status == "READY":
            indexed = True
            # logger.info("File {} is indexed in the OpenCGA study {}.".format(file_name, metadata['study']))
        else:
            indexed = False

        # Check annotation index status
        if annotation_status == "READY":
            annotated = True
            # logger.info("File {} is correctly annotated in the OpenCGA study {}.".format(file_name, metadata['study']))
        else:
            annotated = False

        # Check secondary index status
        if secondary_sample_index_status == "READY":
            sample_index = True
            # logger.info("File {} is correctly annotated in the OpenCGA study {}.".format(file_name, metadata['study']))
        else:
            sample_index = False

    # There is more than one file with this name in this study!
    else:
        logger.error("More than one file in OpenCGA with this name {} in study {}".format(file_name, study))
        sys.exit(1)

    return uploaded, indexed, annotated, sample_index, file_path, sample_ids


def upload_file(opencga_cli, oc, study, file, logger, file_info=dict(), file_path="data/"):
    """
    Uploads a file to the OpenCGA instance and stores it in the file path. It also updates the file to add the
    DNA nexus file ID as attribute
    :param opencga_cli: OpenCGA CLI
    :param oc: OpenCGA client
    :param study: study ID
    :param file: VCF file to upload
    :param file_info: attributes to be added to the file
    :param file_path: directory inside OpenCGA where the file should be stored (default: data/)
    :param logger: logger object to generate logs
    """
    # Run upload using the bash CLI
    process = subprocess.Popen([opencga_cli, "files", "upload", "--input", file, "--study", study,
                                "--catalog-path", file_path, "--parents"], stdout=PIPE, stderr=PIPE, text=True)
    logger.info("CLI executed: {}".format(process.args))
    process.wait()  # Wait until the execution is complete to continue with the program
    stdout, stderr = process.communicate()
    if "ERROR" in stderr:
        logger.error(str(stderr))
        sys.exit(1)
    else:
        logger.info("File {} uploaded successfully.".format(os.path.basename(file)))
        logger.info("\n" + stdout)

    # Update file to contain the provided attributes
    try:
        oc.files.update(study=study, files=os.path.basename(file), data=file_info)
    except Exception as e:
        logger.error("Failed to add the attributes to the file in OpenCGA")


def index_file(oc, study, file, logger, somatic=False, multifile=False):
    """
    Indexes a VCF that has already been uploaded to OpenCGA
    :param oc: OpenCGA client
    :param study: study ID
    :param file: name of the VCF file already uploaded into OpenCGA
    :param logger: logger object to generate logs
    """
    retry = 3
    data_obj = {'file': file}
    if somatic:
        data_obj['somatic'] = True
    if multifile:
        data_obj['loadMultiFileData'] = True
    success = None

    def run_index():
        index_job = oc.variants.run_index(study=study, data=data_obj)
        logger.info("Indexing file {} with job ID: {}".format(file, index_job.get_result(0)['id']))
        oc.wait_for_job(response=index_job.get_response(0))
        job_info = oc.jobs.info(study=study, jobs=index_job.get_result(0)['id'])
        return job_info

    for i in range(retry):
        try:
            status = run_index()
            if status.get_result(0)['execution']['status']['name'] == 'DONE':
                success = True
                logger.info("OpenCGA job index file completed successfully for {}".format(file))
                break
            else:
                logger.error("OpenCGA failed to index file {} with status {}".format(file,
                                                                status.get_result(0)['execution']['status']['name']))
                logger.warning("Failed to index file {}. This operations will be reattempted up to {} times.".format(file, str(retry)))
                logger.warning("Attempt indexing file {} ({}/3)".format(file, i+1))
                continue
        except ValueError as ve:
            logger.error("OpenCGA failed to index file {} with status {}".format(file,
                                                                                status.get_result(0)['execution']['status']['name']))
            logger.exception("OpenCGA failed to index file {}. {}".format(file, ve))
            sys.exit(1)
    return success


def variant_stats_index(oc, study, cohort, logger):
    """
    Computes statistics for each variant (e.g. genotype frequencies). This step is independent of the annotation
    :param oc: OpenCGA client
    :param study: study ID
    :param cohort: cohort to be updated
    :param logger: logger object to generate logs
    """
    if isinstance(cohort, str):
        cohort = [cohort]
    variant_stats_job = oc.operations.index_variant_stats(study=study, data={'cohort': cohort})
    logger.info("Calculating variant stats with job ID: {}".format(variant_stats_job.get_result(0)['id']))
    try:
        oc.wait_for_job(response=variant_stats_job.get_response(0))
        status = oc.jobs.info(study=study, jobs=variant_stats_job.get_result(0)['id'])
        if status.get_result(0)['execution']['status']['name'] == 'DONE':
            logger.info("Variant stats index completed successfully")
        else:
            logger.info(
                "Variant stats index failed with status {}".format(
                    status.get_result(0)['execution']['status']['name']))
    except ValueError as ve:
        logger.exception("OpenCGA failed to calculate variant stats. {}".format(ve))
        sys.exit(1)
    # return variant_stats_job.get_result(0)['id']


def annotate_variants(oc, project, study, logger, delay=True):
    """
    Launches an OpenCGA job to force the annotation of any new variants added to the database. This function will
    wait for the job to finish
    :param oc: OpenCGA client
    :param study: study ID
    :param logger: logger object to generate logs
    :param delay: boolean specifying whether the annotation can be delayed
    """
    annotate_job = oc.variant_operations.index_variant_annotation(project=project, data={})
    logger.info("Annotating new variants in project {} with job ID: {}".format(project,
                                                                               annotate_job.get_result(0)['id']))
    # wait for job to finish
    try:
        oc.wait_for_job(response=annotate_job.get_response(0))
        status = oc.jobs.info(study=project+":"+study, jobs=annotate_job.get_result(0)['id'])
        if status.get_result(0)['execution']['status']['name'] == 'DONE':
            logger.info("OpenCGA job annotate variants completed successfully")
        else:
            logger.info(
                "OpenCGA job annotate variants failed with status {}".format(
                    status.get_result(0)['execution']['status']['name']))
    except ValueError as ve:
        logger.exception("OpenCGA annotation job failed. {}".format(ve))
        sys.exit(1)
    return annotate_job


def secondary_annotation_index(oc, study, logger, delay=True):
    """
    Index data in Solr to be displayed in the variant browser
    :param oc: OpenCGA client
    :param study: study ID
    :param logger: logger object to generate logs
    :param delay: boolean specifying whether the annotation can be delayed
    """
    secondary_annotation_index_job = oc.variant_operations.variant_secondary_annotation_index(study=study, data={})
    logger.info("Indexing study {} in Solr with job ID: {}".format(study, secondary_annotation_index_job.get_result(0)['id']))
    # wait for job to finish
    try:
        oc.wait_for_job(response=secondary_annotation_index_job.get_response(0))
        status = oc.jobs.info(study=study, jobs=secondary_annotation_index_job.get_result(0)['id'])
        if status.get_result(0)['execution']['status']['name'] == 'DONE':
            logger.info("OpenCGA job secondary annotation index completed successfully")
        else:
            logger.info(
                "OpenCGA job secondary annotation index failed with status {}".format(
                    status.get_result(0)['execution']['status']['name']))
    except ValueError as ve:
        logger.exception("OpenCGA secondary annotation index job failed. {}".format(ve))
        sys.exit(1)
    return secondary_annotation_index_job


def sample_variant_stats(oc, study, sample_ids, logger):
    """
    Compute sample variant stats for the selected list of samples
    :param oc: OpenCGA client
    :param study: study ID
    :param sample_ids: list of sample IDs to calculate stats on
    :param logger: logger object to generate logs
    """
    sample_variant_stats_job = oc.variants.run_sample_stats(study=study, data={'sample': 'all',
                                                                               'index': True,
                                                                               'indexId': 'all'})
    # TODO: Add example of query with filters
    logger.info("Computing sample variant stats for {} with job ID: {}".format('all',
                                                                               sample_variant_stats_job.get_result(0)['id']))
    return sample_variant_stats_job.get_result(0)['id']


def secondary_sample_index(oc, study, logger):
    """
    Index data in Solr to be displayed in the variant browser
    :param oc: OpenCGA client
    :param study: study ID
    :param logger: logger object to generate logs
    :param delay: boolean specifying whether the annotation can be delayed
    """
    secondary_sample_index_job = oc.variant_operations.variant_secondary_sample_index(study=study,
                                                                                      data={'sample': ['all'],
                                                                                            'buildIndex': True,
                                                                                            'annotate': True})
    logger.info("Executing sample indexing with job ID: {}".format(secondary_sample_index_job.get_result(0)['id']))
    # wait for job to finish
    # try:
    #     oc.wait_for_job(response=secondary_sample_index_job.get_response(0))
    #     status = oc.jobs.info(study=study, jobs=secondary_sample_index_job.get_result(0)['id'])
    #     if status.get_result(0)['execution']['status']['name'] == 'DONE':
    #         logger.info("OpenCGA job secondary sample index completed successfully")
    #     else:
    #         logger.info(
    #             "OpenCGA job secondary sample index failed with status {}".format(
    #                 status.get_result(0)['execution']['status']['name']))
    # except ValueError as ve:
    #     logger.exception("OpenCGA secondary sample index job failed. {}".format(ve))
    #     sys.exit(1)
    return secondary_sample_index_job


def check_template(oc, study, logger, template):
    """
    Check that the template has the minimum required information. Assumes a zip file is provided.
    :param oc: OpenCGA client
    :param study: study ID
    :param logger: logger object to generate logs
    :param template: template in ZIP compressed format with the metadata to load
    """
    # check manifest
    manifest, samples, individuals, clinical = read_metadata(metadata_file=template, logger=logger)
    return "done"


def load_template(oc, study, logger, template):
    """
    Index data in Solr to be displayed in the variant browser
    :param oc: OpenCGA client
    :param study: study ID
    :param logger: logger object to generate logs
    :param template: boolean specifying whether the annotation can be delayed
    """
    oc.studies.template(study=study, files=template)
    oc.studies.template(study=study, id={}, overwrite=True, resume=True)
    return "done"
