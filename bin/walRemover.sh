#!/bin/sh

# walRemover.sh

#Authors:
#Ahmad Iftekhar : irumman@rummandba.com

PROGNAME=`basename $0`
PROGPATH=`echo $0 | sed -e 's,[\\/][^\\/][^\\/]*$,,'`

. $PROGPATH/../lib/utils.sh

REVISION=1.0

# Common Commands
remote_copy="scp"
remote_shell="ssh"
local_copy="cp -i"
local_rename="mv"
local_remove="rm -f"
local_file_create="touch"

DEBUG=0
DRYRUN=0
PAUSE=0
RESUME=0


#confFile="/opt/msp/pkg/fourspeed/pgWALSync/cfg/pgwalsync.conf"
#slaveconf
#masterOutboundWal
processDir=${PROGPATH}/../process
ackFileForLastWALRecieved=${processDir}/ackLastWALRecieved
listAckWalFiles=${processDir}/listAckWalFiles
listOfFilesToRemove=${processDir}/listOfFilesToRemove
pauseFile=${processDir}/walremover.pause
slaveDiscardFile=${processDir}/SLAVE_DISCARDED
previousAckFile=${processDir}/previousAckFile
pendingRemoveList=${processDir}/pendingRemoveList
noSlaveMode=0


gvStartFromPendingList=0

### Library ###

debugPrint () {
  if [ ${DEBUG} -eq 1 ];
  then
     LOGLINE="WALREMOVER: `date +'%F-%T'`:"
     echo "${LOGLINE} DEBUG: ${1}"
  fi   
} #debugPrint () 


logPrint () {
  LOGLINE="WALREMOVER: `date +'%F-%T'`:"
  if [ ${DRYRUN} -eq 0 ];
  then
    printf "${LOGLINE} ${1}"
  else
    printf "${LOGLINE} :: DRYRUN MODE:: ${1}"
  fi  
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


checkFileIsEmpty () {
 vFilePath=${1}
 vLines=`cat ${vFilePath} | wc -l`
 if [ ${vLines} -eq 0 ];  #file is empty
 then 
   echo "true" 
 else  
   echo "false" 
 fi
}


#### End of Library ###

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
  echo "     --dry     run the program in DRY run mode; Do not remove files"
  echo " "
  echo "     --pause   pause the program"
  echo " "
  echo "     --resume  resume the program"


  echo " "
  echo " EXAMPLES:"
  echo " ========="
  echo "     ./walRemover.sh -h"
  echo "     ./walRemover.sh  -f ../cfg/pgwalsync.conf"
  echo " "
}



	 

checkLocalConfigValue () {
	 checkConifgValues "processDir" "${processDir}"
	 checkConifgValues "ackFileForLastWALRecieved" "${ackFileForLastWALRecieved}"
	 
} # checkLocalConfigValue


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



checkConfigFile () {
		debugPrint "checkConfigFile(): Check for configFile ${confFile} ..."
		if [ ! -z  ${confFile} ] && [ -f ${confFile} ];
		then
		  source ${confFile}
		else
		  logPrint "FATAL:  Missing configuration file ${confFile}\n"
		  critical_error "Stopping Script"
		fi
		checkConifgValues "masterOutboundWal" "${masterOutboundWal}"
		checkConifgValues "slaveConf" "${slaveConf}"
    checkConifgValues "maxWaitTimeForAck" "${maxWaitTimeForAck}"
    checkConifgValues "maxNumberFileToRemove" "${maxNumberFileToRemove}"
    checkConifgValues "maxFileAllowedInOutboundwal" "${maxFileAllowedInOutboundwal}"
    checkConifgValues "removerInitFileCount" "${removerInitFileCount}"
		
		debugPrint "Checked for configFile ... OK"
} #checkConfigFile () {


checkSlavesConf () {
  debugPrint "checkSlavesConf (): Checking salveConf = ${slaveConf}"
  if [ ! -f ${slaveConf} ];
  then
    critical_error "${slaveConf} not found"
  fi
  debugPrint "checkSlavesConf (): File found"
}

createSlaveDiscardFile () {
  debugPrint "createSlaveDiscardFile () ..."
  slaveIP=${1}
  slaveIPtext=`echo ${slaveIP} | sed 's/\./_/g' ` # Modify all . (dots) to _ (underscore)
  eachSlaveDiscardFile=${slaveDiscardFile}_${slaveIPtext}
  logPrint "CRITICAL: Creating ${slaveIP} discard file...\n"
  ${local_file_create} ${eachSlaveDiscardFile}
  logPrint "DONE\n"
  debugPrint "createSlaveDiscardFile () ... DONE"
}

checkForSlaveDiscard () {
  slaveIP=${1}
  ackFileForSlave=${2}

  slaveIPtext=`echo ${slaveIP} | sed 's/\./_/g' ` # Modify all . (dots) to _ (underscore)

  debugPrint "checkForSlaveDiscard () : ${slaveIP} ${ackFileForSlave}"
  ackFileTime=`getFileDateTime ${ackFileForSlave}`
  debugPrint "ackFileTime=${ackFileTime}"
  debugPrint "Calling minuteDiffFromNow "
  ### Time Diff ######
  MPHR=60    # Minutes per hour.
  NOW=$(date +"%Y-%m-%d %H:%M:%S")
	secNOW=$(echo `date -d "${NOW}" +%s`)
	secDT=$(echo `date -d "${ackFileTime}" +%s`)
	secDiff=$(echo `expr ${secNOW} - ${secDT}`)
	let minDiff=${secDiff}/${MPHR}
  debugPrint "secNow=${secNOW}"
	debugPrint "secDT=${secDT}"
  debugPrint "minDiff=${minDiff}"
  
  ackFileMinDiffFromNow=${minDiff}
  debugPrint "ackFileMinDiffFromNow=${ackFileMinDiffFromNow}"
  if [ "${ackFileMinDiffFromNow}" -ge "${maxWaitTimeForAck}" ];
  then 
      logPrint "CRITICAL: Last recieved ack file for ${slaveIP} was ${ackFileMinDiffFromNow} minute ago and config value maxWaitTimeForAck=${maxWaitTimeForAck} minute\n "
      createSlaveDiscardFile ${slaveIP}
  fi  
  debugPrint "checkForSlaveDiscard () : Exiting...OK"
} #checkForSlaveDiscard



generateListOfAckWalFilesForSlave () {
    slaveIP=${1}
    debugPrint "generateListOfAckWalFilesForSlave () : slave = ${slaveIP}"
    slaveIPtext=`echo ${slaveIP} | sed 's/\./_/g' ` # Modify all . (dots) to _ (underscore)
    ackFileForSlave=${ackFileForLastWALRecieved}_${slaveIPtext}
    eachSlaveDiscardFile=${slaveDiscardFile}_${slaveIPtext}
    if [  -f ${eachSlaveDiscardFile} ];
	  then
	     logPrint "CRITICAL: Discarded slave ${slaveIP}\n"
	  else
	     
	    if [ ! -f ${ackFileForSlave} ];
	    then
	      debugPrint "ackFileForSlave = ${ackFileForSlave} not exists"
	      logPrint "WARNING: Acknowledgement not found for slave = ${slaveIP}. Creating a WAITING file\n" 
	      echo "WAITING" >  ${ackFileForSlave}
	    else
	      debugPrint "ackFileForSlave = ${ackFileForSlave} found"
		  fi # if [ -f ${ackFileForSlave} ];
	
		  walFileName=`cat ${ackFileForSlave}` 
		  debugPrint "walFileName = ${walFileName}"
		  if [ ${walFileName} = "WAITING" ];
		  then
		    walFileDateTime='2000-01-01 12:00:00'
	    else
		  	#walFileDateTime=`stat ${walFileName} | grep Modify | awk '{ print $2" "$3}'`
		    walFileDateTime=`getFileDateTime ${walFileName}`
		    debugPrint "walFileDateTime=${walFileDateTime}"
		    if [ -z "${walFileDateTime}" ];
		    then
		      debugPrint "walFileDateTime is empty"
		      walFileDateTime='2000-01-01 12:00:00'
		    fi 
		  fi
		  debugPrint "walFileDateTime = ${walFileDateTime}"
		  
		  checkForSlaveDiscard ${slaveIP} ${ackFileForSlave}
		  
		  debugPrint "generateListOfAckWalFilesForSlave(): File name for eachSlaveDiscardFile = ${eachSlaveDiscardFile}"
		  if [ ! -f ${eachSlaveDiscardFile} ];
		  then
		    debugPrint "Listing ack file in ${listAckWalFiles}"
		    debugPrint "echo -e \"${walFileDateTime}\t${walFileName}\t${slaveIP}\" >> ${listAckWalFiles}"
		  	echo -e "${walFileDateTime}\t${walFileName}\t${slaveIP}" >> ${listAckWalFiles}
		  else
		    logPrint "CRITICAL: Discarded slave ${slaveIP}\n" 
		  fi
		fi #if [  -f ${eachSlaveDiscardFile} ];    
} #generateListOfAckWalFilesForSlave 


makeListToRemoveFilesNoSlaveMode () {
	debugPrint "makeListToRemoveFilesNoSlaveMode(): ..."
  debugPrint "find  ${masterOutboundWal} -name '0*' -type f  | sort -k 1nr > ${listOfFilesToRemove}"
  find  ${masterOutboundWal} -name '0*'  -type f | sort -k 1nr > ${listOfFilesToRemove}         
	debugPrint "DONE"
} 


makeListToRemoveFiles () {
  
  debugPrint "makeListToRemoveFiles () :"
	  
		debugPrint "Sort ${listAckWalFiles}"
		sort -k1,2 ${listAckWalFiles} > ${listAckWalFiles}.tmp
		${local_rename} ${listAckWalFiles}.tmp ${listAckWalFiles}
		debugPrint "Sorting...Done"
		
		debugPrint "Find lowest date/time for WAL recieved acknowledgement"
		walRecievedlowestTime=`head -1 ${listAckWalFiles} | awk ' { print $1" "$2}'`
		debugPrint "walRecievedlowestTime = ${walRecievedlowestTime}"
		
		walRecievedlowestTimeFile=`head -1 ${listAckWalFiles} | awk ' { print $3}'`
		debugPrint "walRecievedlowestTimeFile = ${walRecievedlowestTimeFile}"
		
		walRecievedlowestTimeSlave=`head -1 ${listAckWalFiles} | awk ' { print $4}'`
		debugPrint "walRecievedlowestTimeSlave = ${walRecievedlowestTimeSlave}"
		
		debugPrint "DONE"
		
		if [ -f ${previousAckFile} ]; 
	  then
			 previousAckFileName=`cat ${previousAckFile}`
			 debugPrint "previousAckFileName= ${previousAckFileName}"
		else
		   previousAckFileName="notExists"	 
		fi	 
		debugPrint "walRecievedlowestTimeFile=${walRecievedlowestTimeFile}"
		debugPrint  "previousAckFileName=${previousAckFileName}"
		if [ ${walRecievedlowestTimeFile} = "WAITING" ];
		then
		  logPrint "CRITICAL: WALREMOVER is waiting for acknowledgement from ${walRecievedlowestTimeSlave}\n"
		
		elif [ ${walRecievedlowestTimeFile} = ${previousAckFileName} ];
		then
		  logPrint "WARNING: No file to be removed as last acknowledged file name same as previous execution\n"
		else
			debugPrint "List files to be removed"
			baseFileName=`basename ${walRecievedlowestTimeFile}`
			debugPrint "find  ${masterOutboundWal} ! -newer ${walRecievedlowestTimeFile} ! -name ${baseFileName} -type f  | sort -k 1nr > ${listOfFilesToRemove}"
			find  ${masterOutboundWal} ! -newer ${walRecievedlowestTimeFile} ! -name ${baseFileName} -type f  | sort -k 1nr > ${listOfFilesToRemove}
			debugPrint "echo ${walRecievedlowestTimeFile} > ${previousAckFile}"
			echo ${walRecievedlowestTimeFile} > ${previousAckFile}
			debugPrint "List generated at ${listOfFilesToRemove}"
			
	  fi
  debugPrint "makeListToRemoveFiles (): DONE"
}




checkForPendingList () {
   debugPrint "checkForPendingList (): ..."
   if [ -f ${pendingRemoveList} ] ;
   then
     vFileIsEmpty=`checkFileIsEmpty ${pendingRemoveList}`
     if [ ${vFileIsEmpty} = "false" ];
     then
        lastListedFile=`tail -1 ${pendingRemoveList}`
        if [ -f ${lastListedFile} ];
        then
           logPrint "WARNING: Pending list exists. Generating new list from that...\n"
				   debugPrint "${local_rename}  ${vTmplistOfFilesToRemove} ${listOfFilesToRemove}"
				   cat ${pendingRemoveList} > ${listOfFilesToRemove}
           gvStartFromPendingList=1
           logPrint "DONE\n"
        else
           gvStartFromPendingList=0
        fi
     fi #if [ ${vFileIsEmpty} = "false" ];
   fi #if [ -f ${listOfFilesToRemove} ] ;

   debugPrint "gvStartFromPendingList=${gvStartFromPendingList}"
   debugPrint "checkForPendingList() ... DONE"

} #checkForPendingList



createPendingRemoveList () {
  debugPrint "createPendingRemoveList () ..."
  lineNumber=${1}
  debugPrint "lineNumber=${lineNumber}"
  debugPrint "awk 'NR>"$lineNumber"' ${listOfFilesToRemove} >  ${pendingRemoveList}   "
  awk 'NR>$lineNumber' ${listOfFilesToRemove} >  ${pendingRemoveList}   
  vLines=`cat ${pendingRemoveList} | wc -l`
  debugPrint "Lines in ${pendingRemoveList} = vLines"
  if [ ${vLines} -eq 0 ];
  then
    debugPrint "${pendingRemoveList} is empty and removing"
    ${local_remove} ${pendingRemoveList}
  fi 
  debugPrint "createPendingRemoveList () ...DONE"
} 


removeFiles () {

	numberOfFilesToRemove=`cat ${listOfFilesToRemove} | wc -l`
	debugPrint "removeFiles (): Removing files listed at ${listOfFilesToRemove}"
	i=0
	for eachFile in  `cat ${listOfFilesToRemove}`
	do
	  if [ -f ${eachFile} ];
	  then
		  cmd="${local_remove} ${eachFile}"
		  debugPrint "${cmd}"
		  if [ ${DRYRUN} -eq 0 ];
		  then
		     ${cmd}
		     if [ $? -gt 0 ];
		     then
		        critical_error "Failed to remove file ${eachFile}"
		     fi
		  fi # if [ ${DRYRUN} -eq 0 ];
	    logPrint "LOG: Removing file ${eachFile} ...OK\n"
		  let i=${i}+1
		  if [ ${i} -ge ${maxNumberFileToRemove} ];
		  then
		     logPrint "LOG: Stop removing file as maxNumberFileToRemove (${maxNumberFileToRemove}) reached\n" 
		     createPendingRemoveList ${i}
		     break
		  fi #if [ ${i} -ge ${maxNumberFileToRemove} ];
		else
		  logPrint "WARNING: File ${eachFile} has already been removed\n"
		fi  
	done
	
	debugPrint "removeFiles (): Removed files ${i} of ${numberOfFilesToRemove} "

} #removeFiles () 



checkValidIP () {
  ip=${1}
if expr "$ip" : '[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*$' >/dev/null; then
  echo "true"
else
  echo "false"
fi

}


checkFileThresholdForNoSlaveMode () {
   debugPrint "checkFileThresholdForNoSlaveMode () ..."
   numberOfFileInOutboundwal=`ls -l ${masterOutboundWal} | wc -l `
   let numberOfFileInOutboundwal=${numberOfFileInOutboundwal}-1
   debugPrint "numberOfFileInOutboundwal = ${numberOfFileInOutboundwal}"
   if [  ${numberOfFileInOutboundwal} -ge ${maxFileAllowedInOutboundwal} ];
   then
      logPrint "CRITICAL: maxFileAllowedInOutboundwal (${maxFileAllowedInOutboundwal}) reached. Enabling NOSLAVE mode\n"
      for eachSlave in `cat ${slaveConf}`
      do
        validIP=`checkValidIP ${eachSlave}`
        if [ ${validIP} = 'true' ];
        then
          createSlaveDiscardFile   ${eachSlave}
        fi  
      done
      makeListToRemoveFilesNoSlaveMode 
      removeFiles 
      logPrint "DONE\n"
   fi
   
  debugPrint "checkFileThresholdForNoSlaveMode () ... DONE"

}

exitWithSuccess () {
	logPrint "LOG: Going into sleep mode\n"
	logPrint "LOG: #####################\n"
}


checkOutboundwalFiles () {
   debugPrint "checkOutboundwalFiles  () ..."
   numberOfFileInOutboundwal=`ls -l ${masterOutboundWal} | wc -l `
   let numberOfFileInOutboundwal=${numberOfFileInOutboundwal}-1
   debugPrint "numberOfFileInOutboundwal = ${numberOfFileInOutboundwal}"
   debugPrint "removerInitFileCount=${removerInitFileCount}"
   if [ ${numberOfFileInOutboundwal} -lt ${removerInitFileCount} ];
   then
      logPrint "LOG: Number of files is less than removerInitFileCount(${removerInitFileCount})  in outbound WAL dir (${masterOutboundWal})\n"
      exitWithSuccess 
      exit $STATE_OK
   fi 
}




### Main ###
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


logPrint "LOG: Walking up to remove files\n"

if [ -f ${pauseFile} ];
then
  logPrint "WARNING: ${PROGNAME} paused\n"
  exit $STATE_OK
fi

checkConfigFile
checkLocalConfigValue
checkSlavesConf
checkProcessDir

checkOutboundwalFiles # If number files is less than given threshold then exit to system
checkForPendingList  # If pending list exists then remove using that list

if [ ${gvStartFromPendingList} -eq 0 ];
then

	debugPrint "Empty ${listAckWalFiles}"
	> ${listAckWalFiles}
	
	debugPrint "Empty ${listOfFilesToRemove}"
	> ${listOfFilesToRemove}
	
	debugPrint "Read slaveconf and findSlaveLastWalRecieved () for each slave"
	for eachSlave in ` cat ${slaveConf}`
	do
	  debugPrint "#### Working for slave = ${eachSlave} ###"
	  
	  validIP=`checkValidIP ${eachSlave}`
	  debugPrint "IP validity = ${validIP}"
	  if [ ${validIP} = 'false' ];
	  then
	     logPrint "WARNING: Invalid IP in  ${slaveConf} =  ${eachSlave}\n"
	  else   
	     generateListOfAckWalFilesForSlave  ${eachSlave}
	  fi
	  debugPrint "--- DONE for slave = ${eachSlave} ---"
	done
	debugPrint "Generated listAckWalFiles = ${listAckWalFiles}"
	
	debugPrint "Find number of slaves active"
	numberOfSlaveActive=`cat ${listAckWalFiles} | wc -l`
	debugPrint "numberOfSlaveActive=${numberOfSlaveActive}"
	
	if [ ${numberOfSlaveActive} -eq 0 ];
	then
	   logPrint "CRITICAL: Enabling NoSlave mode...DONE\n"
		 noSlaveMode=1
	fi	 
	
	if [ ${noSlaveMode} -eq 1 ];
	then
	  makeListToRemoveFilesNoSlaveMode
	else
	  makeListToRemoveFiles
	fi
fi #if [ ${gvStartFromPendingList} -eq 0 ];

removeFiles

if [ ${gvStartFromPendingList} -eq 0 ];
then
	if [ -z ${walRecievedlowestTimeFile} ];
	then
	   logPrint "LOG: Removed files in no slave mode\n"
	elif [ ${walRecievedlowestTimeFile} = "WAITING" ];
	then
	   logPrint "LOG: Maintaining slave ${walRecievedlowestTimeSlave} - WAITING for acknowledgement\n"
	else
	   logPrint "LOG: Maintaining slave ${walRecievedlowestTimeSlave} - file kept ${walRecievedlowestTimeFile} (${walRecievedlowestTime}) and after \n"
	fi
else
  logPrint "LOG: Removed files from pending list\n"	
fi #if [ ${gvStartFromPendingList} -eq 0 ];

checkFileThresholdForNoSlaveMode
exitWithSuccess 
exit $STATE_OK


