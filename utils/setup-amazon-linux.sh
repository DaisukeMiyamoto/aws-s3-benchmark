#!/bin/bash

sudo yum install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
sudo yum update
sudo yum install -y git python3-pip emacs
sudo yum install -y nload
sudo pip3 install awscli
sudo pip3 install boto3
sudo pip3 install numpy
