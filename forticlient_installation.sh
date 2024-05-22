#!/bin/sh
#########################################################
## File: optconnect_startup.sh
## Author: Brett Nelson
##
## Purpose: A bash script intended to be run on start of data streaming instances, for Linux Ubuntu 20.04 and above.
## This script updates apt source files to include a keyed forticlient, which is then installed using the apt module.
## Once inside the vpn, a connection to the forticlient network is attempted to be established.
## This script may be used in conjunction with 'scan_clients.sh' once the FortiClient/OptConnect VPN network is joined.
## Proper EC2 configuration (including IAM roles) may be found in the 'AWS STREAMING' documentation.
#########################################################


# ssh bullshit locally, to ignore stupid certificates ssh -o UserKnownHostsFile=/dev/null root@host.ip

### IAM PROFILE/CREDENTIAL INSTANCE INFORMATION ###
### Not currently necessary, but it is possible to directly configure AWS CLI in an EC2 instance using IAM by
### writing these values to a file in ~/aws.config inside the EC2.


#profile_name="default"
#role_arn="arn:aws:iam::1111111111:role/rolename"
#credential_source="Ec2InstanceMetadata"


# If using 'ubuntu' user, adduser steps are not necessary..

sudo passwd $("9687") # necessary to not get locked out of root...
echo "9687" | sudo adduser stream_client  # add new linux user

wget -O - https://repo.fortinet.com/repo/forticlient/7.2/ubuntu/DEB-GPG-KEY | sudo apt-key add -
sudo sed '38 i deb [arch=amd64] https://repo.fortinet.com/repo/forticlient/7.2/ubuntu/ /stable multiverse' /etc/apt/sources.list
sudo apt-get update
sudo apt-get install gnome-keyring  # keyring dependency
sudo apt-get install forticlient  # added to apt/sources.list by 'sed' command.
sudo apt-get install awscli # aws command line interface for data transfer, automatically takes the IAM role assigned.
sudo apt-get install logrotate
## Forticlient and aws client now installed locally

# Install Python Manager
sudo apt-get install python3-pip
sudo apt-get install virtualenv # install venv for python
sudo pip3 install virtualenv


# Install necessary python packages for optclient_stream.sh
# shellcheck disable=SC2117
su stream_client
### -c allows path to be non-su path aka readable by our non-su client.
echo -n "9687" | gnome-keyring-daemon --unlock -c /home/stream_client/keyring
gnome-keyring-daemon --start
# Register fortinet client
forticlient epctrl register 4NGJY1ZV70Q2YLTZRMEMMR1KOIX4K0Z1

# Connect to forticlient
myfortipass="9P3nguin-Int3grat3-Biryani*"
echo """$myfortipass""" | forticlient vpn connect OptClient-VPN -u YousolarDataStreaming -p  -s # use -s to automate log in..

# Disconnect after initial connection.
forticlient vpn disconnect
# Clone project files
git clone https://github.com/throneofshadow/CirrusStream.git

# install python libraries necessary for project
sudo apt-get install cryptography==36.0.0  # Avoid openSSL error (bad ssl w/ crypto <36)
sudo apt-get install pandas awscli paramiko

# get all necessary files for streaming from optclient vpn

aws s3 cp s3://streamingawsbucket/config/client_pass.txt /home/ubuntu/client_pass.txt
aws s3 cp s3://streamingawsbucket/config/opt_pass.txt /home/ubuntu/opt_pass.txt

# Optional: Encrypt passwords

#echo "9P3nguin-Int3grat3-Biryani*" | openssl enc -aes-256-cbc -md sha512 -a -pbkdf2 -iter 100000 -salt -pass pass:9687 > opt_pass.txt
#echo "aeChei2x" | openssl enc -aes-256-cbc -md sha512 -a -pbkdf2 -iter 100000 -salt -pass pass:9687 > client_pass.txt
# Set active controls

chmod 600 client_pass.txt
chmod 600 opt_pass.txt
# exit with status 0 ok.
exit 0