#! /bin/ksh
#----------------------------------------------------------------------------------
#  File:  int_purge_psm_intg.ksh
#
#  Desc:  UNIX shell script to Call respective Functions of  Package INTG_PURGE_DATA_RET  
#-------------------------------------------------------------------------------------
pgmName=`basename $0`
pgmName=${pgmName##*/}    # remove the path
pgmExt=${pgmName##*.}     # get the extension
pgmName=${pgmName%.*}     # get the program name
pgmPID=$$                 # get the process ID
exeDate=`date +"%h_%d"`   # get the execution date
LOGFILE="${LOGDIR}/$exeDate"_ctrl".log"
ERRORFILE="${ERROR}/err.$pgmName."$exeDate
ERRINDFILE=err.ind

# Initialize number of parallel threads
OK=0
FATAL=255

USAGE="Usage: `basename $0`  <connect> \n"

#-------------------------------------------------------------------------
# Function Name: LOG_ERROR
# Purpose      : Log the error messages to the error file.
#-------------------------------------------------------------------------
function LOG_ERROR
{
   errMsg=`echo $1`       # echo message to a single line
   errFunc=$2
   retCode=$3

   dtStamp=`date +"%G%m%d%H%M%S"`
   echo "$pgmName~$dtStamp~$errFunc~$errMsg" >> $ERRORFILE
   if [[ $retCode -eq ${FATAL} ]]; then
      LOG_MESSAGE "Aborted in" $errFunc $retCode
   fi
   return $retCode
}

#-------------------------------------------------------------------------
# Function Name: LOG_MESSAGE
# Purpose      : Log the  messages to the log file.
#-------------------------------------------------------------------------
function LOG_MESSAGE
{
   logMsg=`echo $1`       # echo message to a single line
   logFunc=$2
   retCode=$3

   dtStamp=`date +"%a %b %e %T"`
   echo "$dtStamp Program: $pgmName: PID=$pgmPID: $logMsg $logFunc" >> $LOGFILE
   return $retCode
}

#-------------------------------------------------------------------------
# Function Name: EXEC_SQL_CTRL
# Purpose      : Used for executing the sql statements.
#-------------------------------------------------------------------------

function EXEC_SQL_CTRL
{
   sqlTxt=$*

   sqlReturn=`echo "set feedback off;
      set heading off;
      set term off;
      set verify off;
      set serveroutput on size 1000000;

      VARIABLE GV_return_code    NUMBER;
      VARIABLE GV_script_error   CHAR(255);

      EXEC :GV_return_code  := 0;
      EXEC :GV_script_error := NULL;

      WHENEVER SQLERROR EXIT ${FATAL}
      $sqlTxt
      /

      print :GV_script_error;
      exit  :GV_return_code;
      " | sqlplus -s ${CONNECT}`
    
   if [[ $? -ne ${OK} ]]; then
      LOG_ERROR "${sqlReturn}" "EXEC_SQL_CTRL" ${FATAL} ${ERRORFILE} ${LOGFILE} ${pgmName}
      return ${FATAL}
   fi

   return ${OK}
}


#-------------------------------------------------------------------------
# Function Name: DEL_INT_RETICKET_CTRL_AUDIT --1
# Purpose      : calls the package INTG_PURGE_DATA_RET.DEL_INT_RETICKET_CTRL_AUDIT
#-------------------------------------------------------------------------

function DEL_INT_RETICKET_CTRL_AUDIT
{
   threadVal=1
   sqlTxt="
      DECLARE
         L_str_error_tst   VARCHAR2(1) := NULL;

         FUNCTION_ERROR    EXCEPTION;
      BEGIN

         if NOT INTG_PURGE_DATA_RET.DEL_INT_RETICKET_CTRL_AUDIT(:GV_script_error) then
            raise FUNCTION_ERROR;
         end if;

         COMMIT;

      EXCEPTION
         when FUNCTION_ERROR then
            ROLLBACK;
            :GV_return_code := ${FATAL};
         when OTHERS then
            ROLLBACK;
            :GV_script_error := SQLERRM;
            :GV_return_code := ${FATAL};
      END;"	  
   EXEC_SQL_CTRL ${sqlTxt}

   if [[ $? -ne ${OK} ]]; then
      echo "DEL_INT_RETICKET_CTRL_AUDIT Thread: $threadVal Failed" >>${ERRORFILE}
      return ${FATAL}
   else
      LOG_MESSAGE "Thread ${threadVal} - Successfully Completed"
      return ${OK}
   fi
}

#-------------------------------------------------------------------------
# Function Name: DEL_INT_STAGED_MESSAGE_AUDIT --2
# Purpose      : calls the package INTG_PURGE_DATA_RET.DEL_INT_STAGED_MESSAGE_AUDIT
#-------------------------------------------------------------------------

function DEL_INT_STAGED_MESSAGE_AUDIT
{
   threadVal=1
   sqlTxt="
      DECLARE
         L_str_error_tst   VARCHAR2(1) := NULL;

         FUNCTION_ERROR    EXCEPTION;
      BEGIN

         if NOT INTG_PURGE_DATA_RET.DEL_INT_STAGED_MESSAGE_AUDIT(:GV_script_error) then
            raise FUNCTION_ERROR;
         end if;

         COMMIT;

      EXCEPTION
         when FUNCTION_ERROR then
            ROLLBACK;
            :GV_return_code := ${FATAL};
         when OTHERS then
            ROLLBACK;
            :GV_script_error := SQLERRM;
            :GV_return_code := ${FATAL};
      END;"	  
   EXEC_SQL_CTRL ${sqlTxt}

   if [[ $? -ne ${OK} ]]; then
      echo "DEL_INT_STAGED_MESSAGE_AUDIT Thread: $threadVal Failed" >>${ERRORFILE}
      return ${FATAL}
   else
      LOG_MESSAGE "Thread ${threadVal} - Successfully Completed"
      return ${OK}
   fi
}

#-------------------------------------------------------------------------
# Function Name: DEL_INT_STKADJ_CTRL_AUDIT --3
# Purpose      : calls the package INTG_PURGE_DATA_RET.DEL_INT_STKADJ_CTRL_AUDIT
#-------------------------------------------------------------------------

function DEL_INT_STKADJ_CTRL_AUDIT
{
   threadVal=1
   sqlTxt="
      DECLARE
         L_str_error_tst   VARCHAR2(1) := NULL;

         FUNCTION_ERROR    EXCEPTION;
      BEGIN

         if NOT INTG_PURGE_DATA_RET.DEL_INT_STKADJ_CTRL_AUDIT(:GV_script_error) then
            raise FUNCTION_ERROR;
         end if;

         COMMIT;

      EXCEPTION
         when FUNCTION_ERROR then
            ROLLBACK;
            :GV_return_code := ${FATAL};
         when OTHERS then
            ROLLBACK;
            :GV_script_error := SQLERRM;
            :GV_return_code := ${FATAL};
      END;"	  
   EXEC_SQL_CTRL ${sqlTxt}

   if [[ $? -ne ${OK} ]]; then
      echo "DEL_INT_STKADJ_CTRL_AUDIT Thread: $threadVal Failed" >>${ERRORFILE}
      return ${FATAL}
   else
      LOG_MESSAGE "Thread ${threadVal} - Successfully Completed"
      return ${OK}
   fi
}

#-------------------------------------------------------------------------
# Function Name: DEL_INT_STKCNT_CTRL_AUDIT --4
# Purpose      : calls the package INTG_PURGE_DATA_RET.DEL_INT_STKCNT_CTRL_AUDIT
#-------------------------------------------------------------------------

function DEL_INT_STKCNT_CTRL_AUDIT
{
   threadVal=1
   sqlTxt="
      DECLARE
         L_str_error_tst   VARCHAR2(1) := NULL;

         FUNCTION_ERROR    EXCEPTION;
      BEGIN

         if NOT INTG_PURGE_DATA_RET.DEL_INT_STKCNT_CTRL_AUDIT(:GV_script_error) then
            raise FUNCTION_ERROR;
         end if;

         COMMIT;

      EXCEPTION
         when FUNCTION_ERROR then
            ROLLBACK;
            :GV_return_code := ${FATAL};
         when OTHERS then
            ROLLBACK;
            :GV_script_error := SQLERRM;
            :GV_return_code := ${FATAL};
      END;"	  
   EXEC_SQL_CTRL ${sqlTxt}

   if [[ $? -ne ${OK} ]]; then
      echo "DEL_INT_STKCNT_CTRL_AUDIT Thread: $threadVal Failed" >>${ERRORFILE}
      return ${FATAL}
   else
      LOG_MESSAGE "Thread ${threadVal} - Successfully Completed"
      return ${OK}
   fi
}

#-------------------------------------------------------------------------
# Function Name: DEL_INT_TSFIN_AUDIT --5
# Purpose      : calls the package INTG_PURGE_DATA_RET.DEL_INT_TSFIN_AUDIT
#-------------------------------------------------------------------------

function DEL_INT_TSFIN_AUDIT
{
   threadVal=1
   sqlTxt="
      DECLARE
         L_str_error_tst   VARCHAR2(1) := NULL;

         FUNCTION_ERROR    EXCEPTION;
      BEGIN

         if NOT INTG_PURGE_DATA_RET.DEL_INT_TSFIN_AUDIT(:GV_script_error) then
            raise FUNCTION_ERROR;
         end if;

         COMMIT;

      EXCEPTION
         when FUNCTION_ERROR then
            ROLLBACK;
            :GV_return_code := ${FATAL};
         when OTHERS then
            ROLLBACK;
            :GV_script_error := SQLERRM;
            :GV_return_code := ${FATAL};
      END;"	  
   EXEC_SQL_CTRL ${sqlTxt}

   if [[ $? -ne ${OK} ]]; then
      echo "DEL_INT_TSFIN_AUDIT Thread: $threadVal Failed" >>${ERRORFILE}
      return ${FATAL}
   else
      LOG_MESSAGE "Thread ${threadVal} - Successfully Completed"
      return ${OK}
   fi
}

#-------------------------------------------------------------------------
# Function Name: DEL_INT_TSFIN_CTRL_AUDIT --6
# Purpose      : calls the package INTG_PURGE_DATA_RET.DEL_INT_TSFIN_CTRL_AUDIT
#-------------------------------------------------------------------------

function DEL_INT_TSFIN_CTRL_AUDIT
{
   threadVal=1
   sqlTxt="
      DECLARE
         L_str_error_tst   VARCHAR2(1) := NULL;

         FUNCTION_ERROR    EXCEPTION;
      BEGIN

         if NOT INTG_PURGE_DATA_RET.DEL_INT_TSFIN_CTRL_AUDIT(:GV_script_error) then
            raise FUNCTION_ERROR;
         end if;

         COMMIT;

      EXCEPTION
         when FUNCTION_ERROR then
            ROLLBACK;
            :GV_return_code := ${FATAL};
         when OTHERS then
            ROLLBACK;
            :GV_script_error := SQLERRM;
            :GV_return_code := ${FATAL};
      END;"	  
   EXEC_SQL_CTRL ${sqlTxt}

   if [[ $? -ne ${OK} ]]; then
      echo "DEL_INT_TSFIN_CTRL_AUDIT Thread: $threadVal Failed" >>${ERRORFILE}
      return ${FATAL}
   else
      LOG_MESSAGE "Thread ${threadVal} - Successfully Completed"
      return ${OK}
   fi
}

#-------------------------------------------------------------------------
# Function Name: DEL_INT_SALES_WEEK_SUM --7
# Purpose      : calls the package INTG_PURGE_DATA_RET.DEL_INT_SALES_WEEK_SUM
#-------------------------------------------------------------------------

function DEL_INT_SALES_WEEK_SUM
{
   threadVal=1
   sqlTxt="
      DECLARE
         L_str_error_tst   VARCHAR2(1) := NULL;

         FUNCTION_ERROR    EXCEPTION;
      BEGIN

         if NOT INTG_PURGE_DATA_RET.DEL_INT_SALES_WEEK_SUM(:GV_script_error) then
            raise FUNCTION_ERROR;
         end if;

         COMMIT;

      EXCEPTION
         when FUNCTION_ERROR then
            ROLLBACK;
            :GV_return_code := ${FATAL};
         when OTHERS then
            ROLLBACK;
            :GV_script_error := SQLERRM;
            :GV_return_code := ${FATAL};
      END;"	  
   EXEC_SQL_CTRL ${sqlTxt}

   if [[ $? -ne ${OK} ]]; then
      echo "DEL_INT_SALES_WEEK_SUM Thread: $threadVal Failed" >>${ERRORFILE}
      return ${FATAL}
   else
      LOG_MESSAGE "Thread ${threadVal} - Successfully Completed"
      return ${OK}
   fi
}

#-------------------------------------------------------------------------
# Function Name: DEL_INT_STORE_BATCH_CTRL --8
# Purpose      : calls the package INTG_PURGE_DATA_RET.DEL_INT_STORE_BATCH_CTRL
#-------------------------------------------------------------------------

function DEL_INT_STORE_BATCH_CTRL
{
   threadVal=1
   sqlTxt="
      DECLARE
         L_str_error_tst   VARCHAR2(1) := NULL;

         FUNCTION_ERROR    EXCEPTION;
      BEGIN

         if NOT INTG_PURGE_DATA_RET.DEL_INT_STORE_BATCH_CTRL(:GV_script_error) then
            raise FUNCTION_ERROR;
         end if;

         COMMIT;

      EXCEPTION
         when FUNCTION_ERROR then
            ROLLBACK;
            :GV_return_code := ${FATAL};
         when OTHERS then
            ROLLBACK;
            :GV_script_error := SQLERRM;
            :GV_return_code := ${FATAL};
      END;"	  
   EXEC_SQL_CTRL ${sqlTxt}

   if [[ $? -ne ${OK} ]]; then
      echo "DEL_INT_STORE_BATCH_CTRL Thread: $threadVal Failed" >>${ERRORFILE}
      return ${FATAL}
   else
      LOG_MESSAGE "Thread ${threadVal} - Successfully Completed"
      return ${OK}
   fi
}

#-------------------------------------------------------------------------
# Function Name: DEL_INTG_DSD_UPLOAD_CTRL --9
# Purpose      : calls the package INTG_PURGE_DATA_RET.DEL_INTG_DSD_UPLOAD_CTRL
#-------------------------------------------------------------------------

function DEL_INTG_DSD_UPLOAD_CTRL
{
   threadVal=1
   sqlTxt="
      DECLARE
         L_str_error_tst   VARCHAR2(1) := NULL;

         FUNCTION_ERROR    EXCEPTION;
      BEGIN

         if NOT INTG_PURGE_DATA_RET.DEL_INTG_DSD_UPLOAD_CTRL(:GV_script_error) then
            raise FUNCTION_ERROR;
         end if;

         COMMIT;

      EXCEPTION
         when FUNCTION_ERROR then
            ROLLBACK;
            :GV_return_code := ${FATAL};
         when OTHERS then
            ROLLBACK;
            :GV_script_error := SQLERRM;
            :GV_return_code := ${FATAL};
      END;"	  
   EXEC_SQL_CTRL ${sqlTxt}

   if [[ $? -ne ${OK} ]]; then
      echo "DEL_INTG_DSD_UPLOAD_CTRL Thread: $threadVal Failed" >>${ERRORFILE}
      return ${FATAL}
   else
      LOG_MESSAGE "Thread ${threadVal} - Successfully Completed"
      return ${OK}
   fi
}

#-------------------------------------------------------------------------
# Function Name: DEL_INTG_PRICE_EVENT_CTRL --10
# Purpose      : calls the package INTG_PURGE_DATA_RET.DEL_INTG_PRICE_EVENT_CTRL
#-------------------------------------------------------------------------

function DEL_INTG_PRICE_EVENT_CTRL
{
   threadVal=1
   sqlTxt="
      DECLARE
         L_str_error_tst   VARCHAR2(1) := NULL;

         FUNCTION_ERROR    EXCEPTION;
      BEGIN

         if NOT INTG_PURGE_DATA_RET.DEL_INTG_PRICE_EVENT_CTRL(:GV_script_error) then
            raise FUNCTION_ERROR;
         end if;

         COMMIT;

      EXCEPTION
         when FUNCTION_ERROR then
            ROLLBACK;
            :GV_return_code := ${FATAL};
         when OTHERS then
            ROLLBACK;
            :GV_script_error := SQLERRM;
            :GV_return_code := ${FATAL};
      END;"	  
   EXEC_SQL_CTRL ${sqlTxt}

   if [[ $? -ne ${OK} ]]; then
      echo "DEL_INTG_PRICE_EVENT_CTRL Thread: $threadVal Failed" >>${ERRORFILE}
      return ${FATAL}
   else
      LOG_MESSAGE "Thread ${threadVal} - Successfully Completed"
      return ${OK}
   fi
}


#-----------------------------------------------
# Main program starts 
# Parse the command line
#-----------------------------------------------

# Test for the number of input arguments
if [ $# -lt 2 ]
then
   echo $USAGE
   exit 1
fi

CONNECT=$1
JOBNAME=$2

USER=${CONNECT%/*}


echo "Process Started..." >>$ERRORFILE

$ORACLE_HOME/bin/sqlplus -s $CONNECT <<EOF >>$ERRORFILE
EOF

if [ `cat $ERRORFILE | grep "ORA" | wc -l` -gt 1 ]
then
   echo "Exiting due to ORA/LOGIN Error. Check error file"  >> $LOGFILE
   exit 1;
fi

LOG_MESSAGE "Started by ${USER}"
#Calls to the fucntion based on the thread count

if [ $JOBNAME = "reticket" -o $JOBNAME = "all" -o $JOBNAME = "ALL" ]
then
	DEL_INT_RETICKET_CTRL_AUDIT
	echo "DEL_INT_RETICKET_CTRL_AUDIT OK"  >> $LOGFILE
fi

if [ $JOBNAME = "staged" -o $JOBNAME = "all" -o $JOBNAME = "ALL" ]
then
	DEL_INT_STAGED_MESSAGE_AUDIT
	echo "DEL_INT_STAGED_MESSAGE_AUDIT OK"  >> $LOGFILE
fi

if [ $JOBNAME = "stkadj" -o $JOBNAME = "all" -o $JOBNAME = "ALL" ]
then
	DEL_INT_STKADJ_CTRL_AUDIT
	echo "DEL_INT_STKADJ_CTRL_AUDIT OK"  >> $LOGFILE
fi

if [ $JOBNAME = "stkcnt" -o $JOBNAME = "all" -o $JOBNAME = "ALL" ]
then
	DEL_INT_STKCNT_CTRL_AUDIT
	echo "DEL_INT_STKCNT_CTRL_AUDIT OK"  >> $LOGFILE
fi

if [ $JOBNAME = "tsfin" -o $JOBNAME = "all" -o $JOBNAME = "ALL" ]
then
	DEL_INT_TSFIN_AUDIT
	echo "DEL_INT_TSFIN_AUDIT OK"  >> $LOGFILE
fi

if [ $JOBNAME = "tsfin_ctrl" -o $JOBNAME = "all" -o $JOBNAME = "ALL" ]
then
	DEL_INT_TSFIN_CTRL_AUDIT
	echo "DEL_INT_TSFIN_CTRL_AUDIT OK"  >> $LOGFILE
fi

if [ $JOBNAME = "sales" -o $JOBNAME = "all" -o $JOBNAME = "ALL" ]
then
	DEL_INT_SALES_WEEK_SUM
	echo "DEL_INT_SALES_WEEK_SUM OK"  >> $LOGFILE
fi

if [ $JOBNAME = "store" -o $JOBNAME = "all" -o $JOBNAME = "ALL" ]
then
	DEL_INT_STORE_BATCH_CTRL
	echo "DEL_INT_STORE_BATCH_CTRL OK"  >> $LOGFILE
fi

if [ $JOBNAME = "dsd_upload" -o $JOBNAME = "all" -o $JOBNAME = "ALL" ]
then
	DEL_INTG_DSD_UPLOAD_CTRL
	echo "DEL_INTG_DSD_UPLOAD_CTRL OK"  >> $LOGFILE
fi

if [ $JOBNAME = "price_event" -o $JOBNAME = "all" -o $JOBNAME = "ALL" ]
then
	DEL_INTG_PRICE_EVENT_CTRL
	echo "DEL_INTG_PRICE_EVENT_CTRL OK"  >> $LOGFILE
fi

# Check for any Oracle errors from the SQLPLUS process
if [ `grep "ORA-" $ERRORFILE | wc -l` -gt 0 ]
then
   echo "Exiting due to ORA Error. Check error file"  >> $LOGFILE
   exit 1
else
  rm -f $ERRORFILE
fi

exit 0
