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
status_id = "id"  # Used to be "name" in v2.1, but we are moving to using "id" since v2.2


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
        logger.info("Successfully connected to pyopencga.\nToken ID: {}".format(oc.token))
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
    process = subprocess.run([opencga_cli, "users", "login", "-u", credentials['user']],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                             input=credentials['password'])
    logger.info(process.stdout)
    # Check that the login worked
    if process.stderr != "":
        logger.error("Failed to connect to OpenCGA CLI")
        sys.exit(1)


def check_file_status(oc, study, file_name, attributes, logger, check_attributes=False):
    """
    Perform file checks. First if file has already been uploaded (file name exists in files.search) and if so, check
    if the file has been already indexed and annotated.
    :param oc: openCGA client
    :param study: study ID
    :param file_name: name of the file that wants to be uploaded
    :param attributes: attributes dictionary (keys and values) to be checked
    :return: returns three booleans indicating whether the file has been uploaded, indexed and annotated
    :param logger: logger object to generate logs
    """

    # Init check variables to None
    uploaded = None
    indexed = None
    annotated = None
    secondary_indexed = None
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
        secondary_index_status = file_search.get_result(0)['internal']['variant']['secondaryIndex']['status'][status_id]

        logger.info("File status: {}".format(file_status))
        logger.info("Index status: {}".format(index_status))
        logger.info("Annotation status: {}".format(annotation_status))
        logger.info("Secondary index status: {}".format(secondary_index_status))

        # Check upload status
        if file_status == "READY":
            uploaded = True
            file_path = file_search.get_result(0)['path']
            sample_ids = file_search.get_result(0)['sampleIds']
            # logger.info("File {} already exists in the OpenCGA study {}. "
            #             "Path to file: {}".format(file_name, metadata['study'], file_search.get_result(0)['path']))
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
        if secondary_index_status == "READY":
            secondary_indexed = True
            # logger.info("File {} is correctly annotated in the OpenCGA study {}.".format(file_name, metadata['study']))
        else:
            secondary_indexed = False

    # There is more than one file with this name in this study!
    else:
        logger.error("More than one file in OpenCGA with this name {} in study {}".format(file_name, study))
        sys.exit(1)

    return uploaded, indexed, annotated, secondary_indexed, file_path, sample_ids


def upload_file(opencga_cli, oc, study, file, logger, attributes=dict(), file_path="data/"):
    """
    Uploads a file to the OpenCGA instance and stores it in the file path. It also updates the file to add the
    DNA nexus file ID as attribute
    :param opencga_cli: OpenCGA CLI
    :param oc: OpenCGA client
    :param study: study ID
    :param file: VCF file to upload
    :param attributes: attributes to be added to the file
    :param file_path: directory inside OpenCGA where the file should be stored (default: data/)
    :param logger: logger object to generate logs
    """
    # Run upload using the bash CLI
    process = subprocess.Popen([opencga_cli, "files", "upload", "--input", file, "--study", study,
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
        oc.files.update(study=study, files=os.path.basename(file), data=attributes)
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
    data_obj = {'file': file}
    if somatic:
        data_obj['somatic'] = True
    if multifile:
        data_obj['multifile'] = True
    index_job = oc.variants.run_index(study=study, data=data_obj)
    logger.info("Indexing file {} with job ID: {}".format(file, index_job.get_result(0)['id']))
    try:
        oc.wait_for_job(response=index_job.get_response(0))
        status = oc.jobs.info(study=study, jobs=index_job.get_result(0)['id'])
        if status.get_result(0)['execution']['status']['name'] == 'DONE':
            logger.info("OpenCGA job index file completed successfully")
        else:
            logger.info(
                "OpenCGA job index file failed with status {}".format(
                    status.get_result(0)['execution']['status']['name']))
    except ValueError as ve:
        logger.exception("OpenCGA failed to index the file. {}".format(ve))
        sys.exit(1)


def variant_stats_index(oc, study, cohort, logger):
    """
    Computes statistics for each variant (e.g. genotype frequencies). This step is independent of the annotation
    :param oc: OpenCGA client
    :param study: study ID
    :param cohort: cohort to be updated
    :param logger: logger object to generate logs
    """
    variant_stats_job = oc.operations.index_variant_stats(study=study, data={'cohort': cohort})
    logger.info("Calculating variant stats with job ID: {}".format(variant_stats_job.get_result(0)['id']))
    return variant_stats_job.get_result(0)['id']


def annotate_variants(oc, study, logger, delay=True):
    """
    Launches an OpenCGA job to force the annotation of any new variants added to the database. This function will
    wait for the job to finish
    :param oc: OpenCGA client
    :param study: study ID
    :param logger: logger object to generate logs
    :param delay: boolean specifying whether the annotation can be delayed
    """
    # If delay is true, the function will search for any pending annotation jobs and no new annotation will be
    # launched. Any following jobs will be dependent of this job.
    # If delay is false, an annotation job will be launched regardless of any other annotations
    annotate_job = None
    if delay:
        prev_annotation_jobs = oc.jobs.search(study=study, **{'tool.id': 'variant-annotation-index'}).get_results()
        for paj in prev_annotation_jobs:
            if paj['internal']['status']['id'] == 'PENDING':
                annotate_job = paj
    # delay = False OR no PENDING annotation job
    if annotate_job is None:
        annotate_job = oc.variant_operations.index_variant_annotation(study=study, data={})
        logger.info("Annotating new variants in study {} with job ID: {}".format(study, annotate_job.get_result(0)['id']))
    # wait for job to finish
    try:
        oc.wait_for_job(response=annotate_job.get_response(0))
        status = oc.jobs.info(study=study, jobs=annotate_job.get_result(0)['id'])
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


def sample_variant_stats(oc, study, sample_ids, logger):
    """
    Compute sample variant stats for the selected list of samples
    :param oc: OpenCGA client
    :param study: study ID
    :param sample_ids: list of sample IDs to calculate stats on
    :param logger: logger object to generate logs
    """
    sample_variant_stats_job = oc.variants.run_sample_stats(study=study, data={'sample': sample_ids,
                                                                               'index-id': 'ALL'})
    logger.info("Computing sample variant stats for {} with job ID: {}".format(', '.join(sample_ids),
                                                                               sample_variant_stats_job.get_result(0)['id']))
    return sample_variant_stats_job.get_result(0)['id']


def secondary_index(oc, study, logger, delay=True):
    """
    Index data in Solr to be displayed in the variant browser
    :param oc: OpenCGA client
    :param study: study ID
    :param logger: logger object to generate logs
    :param delay: boolean specifying whether the annotation can be delayed
    """
    # If delay is true, the function will search for any pending secondary index jobs and, if found, no job will be
    # launched. Any following jobs will be dependent of this job.
    # If delay is false, a secondary index job will be launched regardless of any other pending jobs
    secondary_index_job = None
    if delay:
        prev_secondary_index_jobs = oc.jobs.search(study=study, **{'tool.id': 'variant-secondary-index'}).get_results()
        for psij in prev_secondary_index_jobs:
            if psij['internal']['status']['id'] == 'PENDING':
                secondary_index_job = psij
    # delay = False OR no PENDING secondary index job
    if secondary_index_job is None:
        secondary_index_job = oc.variant_operations.secondary_index_variant(study=study, data={})
        logger.info("Indexing study {} in Solr with job ID: {}".format(study, secondary_index_job.get_result(0)['id']))
    # wait for job to finish
    try:
        oc.wait_for_job(response=secondary_index_job.get_response(0))
        status = oc.jobs.info(study=study, jobs=secondary_index_job.get_result(0)['id'])
        if status.get_result(0)['execution']['status']['name'] == 'DONE':
            logger.info("OpenCGA job secondary index completed successfully")
        else:
            logger.info(
                "OpenCGA job secondary index failed with status {}".format(
                    status.get_result(0)['execution']['status']['name']))
    except ValueError as ve:
        logger.exception("OpenCGA secondary index job failed. {}".format(ve))
        sys.exit(1)
    return secondary_index_job


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

    return "done"
