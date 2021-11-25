#!/bin/bash

## This script uploads the VCF into the correspondent study.

# -e flag causes bash to exit at any point if there is any error,
# the -o pipefail flag tells bash to throw an error if it encounters an error within a pipeline,
# the -x flag causes bash to output each line as it is executed -- useful for debugging
set -e -o  pipefail

echo "Now we're uploading the file"

# test
src/opencga-cli/opencga-client-2.1.0-rc2/bin/opencga.sh files upload --help
