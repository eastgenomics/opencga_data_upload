#!/bin/bash

## This script prepares de OpenCGA CLI, login and upload the VCF into the correspondent study.

# -e flag causes bash to exit at any point if there is any error,
# the -o pipefail flag tells bash to throw an error if it encounters an error within a pipeline,
# the -x flag causes bash to output each line as it is executed -- useful for debugging
set -e -o  pipefail

echo "hola"

# unpack opencga
# tar -xzf /opencga-cli/opencga-client-2.1.0-rc2_v1.tar.gz


# test
src/opencga-cli/opencga-client-2.1.0-rc2/bin/opencga.sh users login -u llopez

# login
