#! /bin/sh

REVISION="v1.9.0 (2010-10-11)"

STATE_OK=0
STATE_WARNING=1
STATE_CRITICAL=2
STATE_UNKNOWN=3
STATE_DEPENDENT=4

export PGUSER="postgres"
export PGPASSWORD="1q2w3e4r"
export PGDATA="/opt/pgdata/data8.4"
PGHOME="/opt/msp/pkg/postgres"
PATH="$PGHOME/bin:$PATH"

print_revision () {
    echo "############################################"
    echo "# $1 : $2"
    echo "############################################"
}

critical_error () {
    echo " "
    echo "CRITICAL ERROR! $1"
    echo " "
    exit $STATE_CRITICAL
}


run_cmd () {
    echo "#    $1"

    $1
    rc=`echo $?`
    if [ ! "$rc" == "0" ]; then
        critical_error "Stopping Script"
    fi
  
    return 0
}



checkConifgValues () {
  configKey=${1}
  configValue=${2}
  debugPrint "checkConifgValues(): Checking coniguration value for ${configKey}=${configValue}"
  if [ -z ${configValue} ]; 
	then
	  logPrint "FATAL:  Missing value for \"${configKey}\"  \n"
		critical_error "Stopping Script"
	else
		  debugPrint "${configKey} = ${configValue}"
	fi
} #checkConifgValues


