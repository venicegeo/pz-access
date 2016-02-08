#!/bin/bash -ex

pushd `dirname $0` > /dev/null
base=$(pwd -P)
popd > /dev/null

# Gather some data about the repo
source $base/vars.sh

# Send a null Data check
[ `curl -s -o /dev/null -w "%{http_code}" http://pz-access.cf.piazzageo.io/data/data` = 200 ]
