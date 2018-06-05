#!/bin/bash

set -x

eval "$(ssh-agent -s)"
ssh-add <(echo "${IMPALA_CLUSTER_PRIVATE_KEY_BASE64}" | base64 --decode)
ssh-add -l

ssh -o "StrictHostKeyChecking=no" ec2-user@ec2-52-40-86-190.us-west-2.compute.amazonaws.com ping -c 1 nonworker1.hadoop.jsaw.io || exit 0
ssh -o "StrictHostKeyChecking=no" -M -S my-ctrl-socket -fnNT -L 21000:nonworker1.hadoop.jsaw.io:21000 ec2-user@ec2-52-40-86-190.us-west-2.compute.amazonaws.com

env | sort

travis_wait 40 time bundle exec rake
result=$?

ssh -S my-ctrl-socket -O exit ec2-user@ec2-52-40-86-190.us-west-2.compute.amazonaws.com

set +x

exit $result
