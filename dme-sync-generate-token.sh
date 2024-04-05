#! /bin/bash

read -p "username: "  username
read -s -p "password: " pswd
echo
read -p "environment: " env

if [ "$env" == "local" ]
    then
        server="https://localhost:7738/hpc-server"
    elif [ "$env" == "dev" ]
    then
        server="https://fsdmel-dsapi01d.ncifcrf.gov:7738/hpc-server"
    elif [ "$env" == "uat" ]
    then
        server="https://fsdsgl-dmeap01t.ncifcrf.gov:7738/hpc-server"
	elif [ "$env" == "prod" ]
    then
        server="https://hpcdmeapi.nci.nih.gov:8080"
    else
        echo "ERROR: Unknown environment:$env"
        exit 1
fi

curl -k -u $username:$pswd  ${server}/authenticate >  ./token_file
