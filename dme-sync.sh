#! /bin/bash

if [ -z "$1" ]
	then
		echo "USAGE: sh dme-sync.sh [env]"
		exit 1
fi

export env=$1

if [ "$env" != "dev" ] && [ "$env" != "uat" ] && [ "$env" != "prod" ]
    then
        echo "ERROR: Unknown environment: $env"
        exit 1
fi


 java -jar dme-sync-2.2.0.jar \
	$env
