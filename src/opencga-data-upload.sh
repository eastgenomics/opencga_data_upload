#!/bin/bash

main() {
    echo "Summary of data provided:"
    echo "- Input VCF(s): ${input_vcf}"
    echo "- Metadata: '${input_metadata}'"
    echo "- Credentials file: '${input_credentials}'"
    echo "- Project: '${input_project}'"
    echo "- Study: '${input_study}'"
    echo "- Somatic: '${input_somatic}'"
    echo "- Multifile: '${input_mutifile}'"

    # Define a function for reading credentials.json
    read_cred () {
        user=$(jq .user "${1}")
        password=$(jq .password "${1}")
        host=$(jq .host "${1}")
        opencga_cli_file_id=$(jq -r .opencga_cli_file_id "${1}")
    }

    # Download inputs in the "in" folder
    mkdir -p /home/dnanexus/in

    # Get the original name of the VCF file
    vcf_name=$(dx describe "${input_vcf}" --name)
    echo "${vcf_name}"

    # Download inputs in parallel
    time dx-download-all-inputs --parallel

    # array inputs end up in subdirectories (i.e. ~/in/array-input/0/), flatten to parent dir
    find ~/in/input_vcf -type f -name "*" -print0 | xargs -0 -I {} mv {} ~/in/input_vcf
    find ~/in/input_metadata -type f -name "*" -print0 | xargs -0 -I {} mv {} ~/in/input_metadata
    # location of input_credentials is ~/in/input_credentials/

    # Read credentials file
    read_cred ~/in/input_credentials/${input_credentials_name}

    # Download openCGA CLI and uncompress
    echo "Getting the OpenCGA CLI"
    # cli file id is stored in the credentials file
    dx download ${opencga_cli_file_id}
    cli_name=$(dx describe "${opencga_cli_file_id}" --name)
    mkdir -p /home/dnanexus/opencga_cli && tar -xzf ${cli_name} -C /home/dnanexus/opencga_cli --strip-components 1
    opencga_cli=$(ls /home/dnanexus/opencga_cli/bin)
    if [ "${opencga_cli}" != "opencga.sh" ]; then
      dx-jobutil-report-error "opencga.sh not found in the provided cli folder. As a result no further actions can be performed"
    else
      echo "${opencga_cli} in ${cli_name} is ready to use"
    fi

    # Get DNAnexus file ID
    echo "Obtaining VCF file ID"
    dnanexus_fid=$(dx describe "${input_vcf}" --json | jq -r '.id')

    # Install python dependencies
    echo "Installing requirements"
    pip install pyopencga-2.4.9-py3-none-any.whl

    # Gather all vcfs for passing to the python script and build string to pass
    vcf_string=$(find ~/in/input_vcf/ -type f)

    # Run opencga load
    echo "Launching OpenCGA upload"
    opencga_cmd="python3 opencga_upload_and_index.py --credentials ~/in/input_credentials/${input_credentials_name} \
                                                     --vcf ${vcf_string} \
                                                     --cli /home/dnanexus/opencga_cli/bin/opencga.sh \
                                                     --dnanexus_fid ${dnanexus_fid} "
    if [ -n "${input_metadata}" ]; then
      # Gather metadata files and build string to pass
      metadata_string+=" --metadata "

      metadata_string=$(find ~/in/input_metadata/ -type f)

      opencga_cmd+=" ${metadata_string}"
    fi
    if [ -n "${input_project}" ]; then
      opencga_cmd+=" --project ${input_project}"
    fi
    if [ -n "${input_study}" ]; then
      opencga_cmd+=" --study ${input_study}"
    fi
    if [ "${input_somatic}" = true ]; then
      opencga_cmd+=" --somatic"
    fi
    if [ "${input_multifile}" = true ]; then
      opencga_cmd+=" --multifile"
    fi
    echo "${opencga_cmd}"
    eval "${opencga_cmd}"

    if [ -f /home/dnanexus/opencga_loader.err ]; then
        if [ -s /home/dnanexus/opencga_loader.err ]; then
            cat
                dx-jobutil-report-error "ERROR: Failed to load VCF ${vcf_name} into OpenCGA. See
                /home/dnanexus/opencga_loader.err for more details."
        else
            echo "VCF ${vcf_name} was loaded successfully to OpenCGA"
        fi
    fi

    opencga_out=$(dx upload /home/dnanexus/opencga_loader.out --brief)
    opencga_err=$(dx upload /home/dnanexus/opencga_loader.err --brief)

    dx-jobutil-add-output opencga_out "${opencga_out}" --class=file
    dx-jobutil-add-output opencga_err "${opencga_err}" --class=file
}
