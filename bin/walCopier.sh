#!/bin/sh


# walCopier.sh

#Authors:
#Ahmad Iftekhar : irumman@rummandba.com


PROGNAME=`basename $0`
PROGPATH=`echo $0 | sed -e 's,[\\/][^\\/][^\\/]*$,,'`

. $PROGPATH/../lib/utils.sh

REVISION=1.0

# Common Commands
remote_copy="scp -q"
remote_shell="ssh -q"
compression_cmd="gzip -c"

#confFile="/opt/msp/pkg/fourspeed/pgWALSync/cfg/pgwalsync.conf"


processDir=${PROGPATH}/../process
listOfWalFiles=${processDir}/listOfWalFiles
storeLastWALFileRecieved=${processDir}/storeLastWALFileRecieved
ackFileForLastWALRecieved=${processDir}/ackLastWALRecieved
pauseFile=${processDir}/walcopier.pause


DEBUG=0
DRYRUN=0
PAUSE=0
RESUME=0


print_usage() {
  echo "Usage: $PROGNAME  -f configFileFullPath"
  return 0
}


print_help() {
  print_revision "$PROGNAME" "$REVISION"
  echo " "
  print_usage
  echo " "
  echo " REQUIRED:"
  echo " ========="
  echo "    \$1   configuration file name with the full path"
  echo " "
  echo " "
  echo " OPTIONAL:"
  echo " ========="
  echo "     -?        print short usage statement and exit"
  echo " "
  echo "     -h        print this help message and exit"
  echo " "
  echo "     -V        print this programs version number info and exit"
  echo " "
  echo "     --debug   run the program in debug mode"
  echo " "
  echo "     --dry     run the program in DRY run mode; Do not copy files from master"
  echo " "
  echo "     --pause   pause the program"
  echo " "
  echo "     --resume  resume the program"

  echo " "
  echo " EXAMPLES:"
  echo " ========="
  echo "     ./walCopier.sh -h"
  echo "     ./walCopier.sh  -f ../cfg/pgwalsync.conf"
  echo " "
}

debugPrint () {
  if [ ${DEBUG} -eq 1 ];
  then
     LOGLINE="WALCOPIER: `date +'%F-%T'`:"
     echo "${LOGLINE} DEBUG: ${1}"
  fi   
} #debugPrint () 


logPrint () {
  LOGLINE="WALCOPIER: `date +'%F-%T'`:"
  if [ ${DRYRUN} -eq 0 ];
  then
    printf "${LOGLINE} ${1}"
  else
    printf "${LOGLINE} :: DRYRUN MODE:: ${1}"
  fi  
}

getFileDateTime () {
	fileName=$1
	if [ -f ${fileName} ];
	then
		dt=`stat ${fileName} | grep Modify | awk '{ print $2" "$3}'`
		echo $dt  
	else
	  echo ""
	fi   	
# Return $dt; call with assign to a variable like dt=`getFileDateTime ${fileName} 
}


checkRemoteDirExists () {
 remoteHost=$1
 remoteDir=$2 
 ${remote_shell} $remoteHost [[ -d $remoteDir ]] && echo "true" || echo "false";
}


checkRemoteFileExists () {
 remoteHost=$1
 remoteFile=$2 
 ${remote_shell} -q $remoteHost [[ -f $remoteFile ]] && echo "true" || echo "false";
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
		  debugPrint "${configKey}=${configValue}"
	fi
  
} #checkConifgValues

checkConfigFile () {
		debugPrint "checkConfigFile(): Check for configFile ${confFile} ..."
		if [ ! -z  ${confFile} ] && [ -f ${confFile} ];
		then
		  source ${confFile}
		else
		  logPrint "FATAL:  Missing configuration file ${confFile}\n"
		  critical_error "Stopping Script"
		fi
		checkConifgValues "masterHost" "${masterHost}"
		checkConifgValues "masterOutboundWal" "${masterOutboundWal}"
		checkConifgValues "localInboundWal" "${localInboundWal}"
    checkConifgValues "maxNumberFileToCopy" "${maxNumberFileToCopy}"
    checkConifgValues "compressedCopy" "${compressedCopy}"

		debugPrint "Checked for configFile ... OK"
} #checkConfigFile () {

checkLocalConfigValue () {
	 checkConifgValues "processDir" "${processDir}"
	 checkConifgValues "listOfWalFiles" "${listOfWalFiles}"
	 checkConifgValues "storeLastWALFileRecieved" "${storeLastWALFileRecieved}"
	 checkConifgValues "ackFileForLastWALRecieved" "${ackFileForLastWALRecieved}"
	 checkConifgValues "pauseFile" "${pauseFile}"
} # checkLocalConfigValue


listWalFilesFromMaster () {
  if [ -f ${listOfWalFiles} ];
  then
    debugPrint " > ${listOfWalFiles} "
    > ${listOfWalFiles}
  fi

  if [  -z ${lastWALFileRecieved} ]; 
  then
    newer=""
  else
    lastWALFileRecievedExists=`checkRemoteFileExists  ${masterHost} ${lastWALFileRecieved}`
    debugPrint "lastWALFileRecievedExists=${lastWALFileRecievedExists}"
    if [ ${lastWALFileRecievedExists} = 'false' ];
    then
      newer=""
      critical_error "${lastWALFileRecieved} not exits at Master. May be salve has been discarded. Please check"
    else  
      newer=" -newer '${lastWALFileRecieved}' " 
    fi  
  fi
  debugPrint "${newer}"
  debugPrint "${remote_shell}  $masterHost  \"find ${masterOutboundWal} -name '0*'  -type f  ${newer} | sort -k 1nr  \" > ${listOfWalFiles}"
  ${remote_shell}  $masterHost  "find ${masterOutboundWal} -name '0*'  -type f  ${newer} | sort -k 1nr  " > ${listOfWalFiles}
  if [ $? -gt 0 ];
  then
     logPrint "FATAL: Cannot get list of WAL files from Master\n"
     critical_error "Stopping Script"
  fi   
  
} #listWalFilesFromMaster ()


checkProcessDir () {
	if [ ! -d ${processDir} ];
	then
	   debugPrint "Creating process directory..."
	   debugPrint "mkdir -p ${processDir}"
	   
	   logPrint "WARNING: processDir = \"${processDir}\" does not exist, creating\n"
	   mkdir -p ${processDir}
	   
	   debugPrint "Done"
	fi  #if [ ! -d ${processDir} ];
} # checkProcessDir () {

makeLocalInboundDir () {
  debugPrint "makeLocalInboundDir(): Check for localInboundWal=${localInboundWal}"
  if [ ! -d $localInboundWal ] 
  then
    debugPrint "Creating ${localInboundWal}"
    logPrint "WARNING: ${localInboundWal} does not exist, creating\n"
    mkdir -p $localInboundWal
  fi
} #makeLocalInboundDir


findLastWalFileRecieved () {
	debugPrint "findLastWalFileRecieved(): Finding last WAL file recieved ..."
	debugPrint "storeLastWALFileRecieved=${storeLastWALFileRecieved}"
	if  [ -f ${storeLastWALFileRecieved} ];
	then 
	  lastWALFileRecieved=`cat ${storeLastWALFileRecieved} | awk ' { print $1 } '`
	else
	  lastWALFileRecieved=""
	fi
} #findLastWalFileRecieved

ackMaster () {
  debugPrint "ackMaster (): Acknowledge master"
  lastWalFile=`cat ${storeLastWALFileRecieved}`
  
  localSlaveIP=`hostname -I`
	debugPrint "localSlaveIP=${localSlaveIP}"
	localSlaveIP=`echo ${localSlaveIP} | sed 's/\./_/g' ` # Modify all . (dots) to _ (underscore)
	
	ackFileForLastWALRecieved=${ackFileForLastWALRecieved}_${localSlaveIP}
  debugPrint "ackFileForLastWALRecieved = ${ackFileForLastWALRecieved}"
  
  debugPrint "echo ${lastWalFile} > ${ackFileForLastWALRecieved}"
  echo ${lastWalFile} > ${ackFileForLastWALRecieved}
  
  debugPrint "${remote_copy}  ${ackFileForLastWALRecieved}  ${masterHost}:${processDir}/"
  ${remote_copy}  ${ackFileForLastWALRecieved}  ${masterHost}:${processDir}/
  if  [ $? -gt 0 ];
  then
    critical_error "FATAL: Not able to acknowledge master\n"
  fi
  logPrint "LOG: Acknowledged to master with last recieved \"${lastWalFile}\"\n" 
  debugPrint "ackMaster (): OK"
  
}


checkMasterDir () {
  debugPrint "checkMasterDir () ..."
  dirName=${1}
  debugPrint "masterHost=${masterHost}"
  debugPrint "dirName=${dirName}"
	masterDirExist=`checkRemoteDirExists ${masterHost} ${dirName}`
	debugPrint "masterDirExist=${masterDirExist}"
	if  [ ${masterDirExist} = "false" ];
	then
	   critical_error "FATAL: ${dirName} not exists at Master (${masterHost})"
	fi
	debugPrint "checkMasterDir () ...DONE"
}



checkValidIP () {
  ip=${1}
if expr "$ip" : '[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*$' >/dev/null; then
  echo "true"
else
  echo "false"
fi
}

checkMaster () {
  debugPrint "checkMaster () : " 
  validIP=`checkValidIP ${masterHost}`
  debugPrint "Check for valid master IP address"
  if [ ${validIP} = "false" ];
  then
     critical_error "Invalid IP for masterHost = ${masterHost}"
  fi
  
  debugPrint "Check establish communication with master"
  ${remote_shell} ${masterHost} "ls" > /dev/null
  if [ $? -gt 0 ];
  then
    critical_error "Cannot establish communication with master ${masterHost}"
  fi  
  debugPrint "checkMaster () : ... DONE" 

}


#### Main #####

if [ $# -eq 0 ];
then 
  print_help
	critical_error "You must specify the required parameters"
	exit $STATE_OK
fi

while [ $# -gt 0 ]; 
do 
	case "$1" in
	  -h)
	      print_help
	      exit $STATE_OK
	      ;;
	  -V)
	      print_revision "$PROGNAME" "$REVISION"
	      exit $STATE_OK
	      ;;
	  -\?)
	      print_usage
	      exit $STATE_OK
	      ;;
	  --debug)
	      DEBUG=1 
	       ;; 
	  --dry)
	       DRYRUN=1 
	       ;; 
	  --pause)
	      PAUSE=1 
	      ;;     
	  --resume)
	      RESUME=1 
	      ;;     
	
	  -f )
	     shift
	     confFile=$1   
	     ;;            
	  *)   
	      print_help
	      critical_error "Unknown parameter specified"
	      exit $STATE_OK
	      ;;
	esac
shift
done

if [ ${RESUME} -eq 1 ] && [ ${PAUSE} -eq 1 ];
then
  print_help 
  critical_error "--pause and --resume are mutually exclusive"
  exit $STATE_OK
fi

#Resume
if [ ${RESUME} -eq 1  ];
then
 if [ -f ${pauseFile} ];
 then
	 logPrint "WARNING: Resuming process\n"
	 debugPrint "Removing ${pauseFile}"
	 ${local_remove} ${pauseFile}
 else
   logPrint "WARNING: Process already running\n"
 fi	 #if [ -f ${pauseFile} ]
fi 

#Pause
if [ ${PAUSE} -eq 1 ];
then
  if [  -f ${pauseFile} ];
  then
     logPrint "WARNING: Process already paused\n"
  else    
	  logPrint "WARNING: Pause process\n"
	  debugPrint "Creaing file ${pauseFile}"
	  ${local_file_create} "${pauseFile}"
  fi
fi

logPrint "LOG: Walking up to copy files\n"
if [ -f ${pauseFile} ];
then
  logPrint "WARNING: ${PROGNAME} paused\n"
  exit $STATE_OK
fi

checkConfigFile
checkLocalConfigValue
checkProcessDir

checkMaster
checkMasterDir ${processDir}
checkMasterDir  ${masterOutboundWal}

makeLocalInboundDir 
findLastWalFileRecieved

debugPrint "Generating  listWalFilesFromMaster ..." 
if [ -z ${lastWALFileRecieved} ];
then
  debugPrint "No WAL file recieved so far"
  listWalFilesFromMaster 
else
  debugPrint "lastWALFileRecieved=${lastWALFileRecieved}"
  listWalFilesFromMaster ${lastWALFileRecieved}
fi
numberOfFiles=` cat ${listOfWalFiles} | wc -l `
debugPrint "Number of files listed =${numberOfFiles}"
debugPrint "Generating  listWalFilesFromMaster ... Done"

 
if [ ${numberOfFiles} -gt 0 ];
then

	debugPrint "Start copying files ..."
	i=0
	for walFileName in ` cat ${listOfWalFiles}  `
	do
	  if [ -f ${pauseFile} ];
    then
      logPrint "WARNING: ${PROGNAME} paused\n"
      break
    fi
    
	  logPrint "LOG: Copying File: ${walFileName}\n"
	  
	  if [ ${compressedCopy} = "true" ];
	  then
	     debugPrint "Compressed Copy"
	     baseFileName=`basename ${walFileName}`
	     debugPrint "${remote_shell} ${masterHost} \"gzip -c ${walFileName}\" | gunzip > ${localInboundWal}/${baseFileName}"
			 if [ ${DRYRUN} -eq 0 ]; 
			 then
         ${remote_shell} ${masterHost} "gzip -c ${walFileName}" | gunzip > ${localInboundWal}/${baseFileName}
		     if [ $? -gt 0 ]; 
		     then
		       critical_error "FATAL: Failed to copy ${walFileName} with compression from master"
		     fi
	     fi #if [ ${DRYRUN} -eq 0 ]; 
	  else
	      debugPrint "Normal copy"
			  debugPrint "${remote_copy}  ${masterHost}:${walFileName} ${localInboundWal}"
			  if [ ${DRYRUN} -eq 0 ]; 
			  then
			     ${remote_copy}  ${masterHost}:${walFileName} ${localInboundWal}
			     if [ $? -gt 0 ]; 
			     then
			       critical_error "FATAL: Failed to copy ${walFileName} from master"
			     fi
			  fi 
	  fi # if [ ${compressedCopy} = "true" ]
		
		let i=${i}+1
		debugPrint "Copied file Number ${i}"
		if [ $i -ge ${maxNumberFileToCopy} ];
		then
			logPrint "LOG: Stop copying file as maxNumberFileToCopy (${maxNumberFileToCopy}) reached\n" 
		break
		fi

	    
	  logPrint  "OK\n"  
	  
	  debugPrint "Saving last recieved WAL file names"
	  
	  debugPrint "echo ${walFileName} > ${storeLastWALFileRecieved}" 
	  echo ${walFileName} > ${storeLastWALFileRecieved}
	done
	debugPrint "Done"
	ackMaster	
else
  logPrint "WARNING: No WAL file found to be copied\n"
fi
logPrint "LOG: Going into sleep mode\n"
logPrint "LOG: #####################\n"

exit $STATE_OK