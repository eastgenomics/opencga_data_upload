#!/bin/bash

main() {
    echo "Summary of data provided:"
    echo "- Input VCF(s): ${vcfs}"
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

    # Download inputs in parallel
    time dx-download-all-inputs --parallel

    # array inputs end up in subdirectories (i.e. ~/in/array-input/0/), flatten to parent dir
    find ~/in/vcfs -type f -name "*" -print0 | xargs -0 -I {} mv {} ~/in/vcfs
    find ~/in/input_metadata -type f -name "*" -print0 | xargs -0 -I {} mv {} ~/in/input_metadata
    # location of input_credentials is ~/in/input_credentials/

    # Read credentials file
    read_cred ~/in/input_credentials/${input_credentials_name}

    # unpack opencga cli
    dx download ${opencga_cli_file_id} -o opencga_client.tar.gz
    mkdir opencga_client && tar -zxf opencga_client.tar.gz -C opencga_client --strip-components 1

    # Install python dependencies
    echo "Installing requirements"
    sudo -H python3 -m pip install --no-index --no-deps packages/*.whl

    # install dxpy
    tar xzf packages/dxpy-0.333.0.tar.gz
    python3 dxpy-0.333.0/setup.py install

    # Gather all vcfs for passing to the python script and build string to pass
    vcf_string=$(find ~/in/vcfs/ -type f -printf "%p ")

    # Run opencga load
    echo "Launching OpenCGA upload"
    opencga_cmd="python3 opencga_upload_and_index.py --credentials ~/in/input_credentials/${input_credentials_name}"
    opencga_cmd+=" --cli /home/dnanexus/opencga_client/bin/opencga.sh"
    opencga_cmd+=" --dnanexus_project ${DX_PROJECT_CONTEXT_ID}"
    opencga_cmd+=" --vcf ${vcf_string}"

    if [ -n "${input_metadata}" ]; then
      # Gather metadata files and build string to pass
      metadata_string=" --metadata "
      metadata_string+=$(find ~/in/input_metadata/ -type f -printf "%p ")
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
                dx-jobutil-report-error "ERROR: Failed to load VCFs into OpenCGA. See
                /home/dnanexus/opencga_loader.err for more details."
        else
            echo "VCFs were loaded successfully to OpenCGA"
        fi
    fi

    opencga_out=$(dx upload /home/dnanexus/opencga_loader.out --brief)
    opencga_err=$(dx upload /home/dnanexus/opencga_loader.err --brief)

    dx-jobutil-add-output opencga_out "${opencga_out}" --class=file
    dx-jobutil-add-output opencga_err "${opencga_err}" --class=file
}
