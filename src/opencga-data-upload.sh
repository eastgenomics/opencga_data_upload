#!/bin/bash
# opencga-data-upload 0.0.1

main() {
    echo "Summary of data provided:"
    echo "- Input VCF(s): ${input_vcf}"
    echo "- Metadata: '${input_metadata}'"
    echo "- Credentials file: '${input_credentials}'"

    # Define a function for reading credentials.json
    read_cred () {
        user=$(jq .user "${1}")
        password=$(jq .password "${1}")
        host=$(jq .host "${1}")
    }

    # Make required folders
    mkdir -p packages

    # Unpack and install python dependencies
    tar xf python_packages.tar.gz -C packages
    python3 -m pip install --no-index --no-deps packages/*


    echo "Downloading input files"
    dx-download-all-inputs --parallel

    #Add all inputs in the same folder to enable indeces to be found.
    find ~/in -type f -name "*" -print0 | xargs -0 -I {} mv {} ~/

    # Read credentials file
    read_cred /home/dnanexus/credentials.json

    # Download openCGA CLI and uncompress
    echo "Getting the OpenCGA CLI"
    mkdir -p /home/dnanexus/opencga_cli && tar -xzf ${opencga_client_name} -C /home/dnanexus/opencga_cli --strip-components 1
    opencga_cli=$(ls /home/dnanexus/opencga_cli/bin)
    if [ "${opencga_cli}" != "opencga.sh" ]; then
      dx-jobutil-report-error "opencga.sh not found in the provided cli folder. As a result no further actions can be performed"
    else
      echo "${opencga_cli} in ${opencga_client_name} is ready to use"
    fi

    # Get DNAnexus file ID
    echo "Obtaining VCF file ID"
    dnanexus_fid=$(dx describe "${input_vcf}" --json | jq -r '.id')


    # Run opencga load
    echo "Launching OpenCGA upload"
    opencga_cmd="python3 opencga_upload_and_index.py --metadata /home/dnanexus/metadata.json \
                                                     --credentials /home/dnanexus/credentials.json \
                                                     --vcf /home/dnanexus/${input_vcf_name} \
                                                     --cli /home/dnanexus/opencga_cli/bin/opencga.sh \
                                                     --dnanexus_fid ${dnanexus_fid}"
    echo "${opencga_cmd}"
    eval "${opencga_cmd}"

    # To report any recognized errors in the correct format in
    # $HOME/job_error.json and exit this script, you can use the
    # dx-jobutil-report-error utility as follows:

    if [ -f /home/dnanexus/opencga_loader.err ]; then
        if [ -s /home/dnanexus/opencga_loader.err ]; then
            cat
                dx-jobutil-report-error "ERROR: Failed to load VCF ${input_vcf_name} into OpenCGA. See
                /home/dnanexus/opencga_loader.err for more details."
        else
            echo "VCF ${input_vcf_name} was loaded successfully to OpenCGA"
        fi
    fi

    ls

    # Upload output
    opencga_out=$(dx upload /home/dnanexus/opencga_loader.out --brief)
    opencga_err=$(dx upload /home/dnanexus/opencga_loader.err --brief)

    dx-jobutil-add-output opencga_out "${opencga_out}" --class=file
    dx-jobutil-add-output opencga_err "${opencga_err}" --class=file
}
