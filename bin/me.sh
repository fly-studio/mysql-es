#!/bin/sh
#
## Example from: https://gist.github.com/gythialy/8e720aa0a43bd0dd0b6a
#

## Get the current path
current_path=`pwd`
case "`uname`" in
    Linux)
		bin_abs_path=$(readlink -f $(dirname $0))
		;;
	*)
		bin_abs_path=`cd $(dirname $0); pwd`
		;;
esac

## Custom

BASE=${bin_abs_path}/..
RUNNING_USER=root
CONF=$BASE/conf
APP_MAINCLASS=com.fly.sync.Main
JAVA_OPTS=" -Djava.awt.headless=true -XX:MaxPermSize=512m -Dme.etc.path=$CONF"
CLASSPATH=$BASE/lib

# Loop for lib/*.jar
for i in "$BASE"/lib/*.jar; do
   CLASSPATH="$CLASSPATH":"$i"
done

## Get java path
if [ -z "$JAVA" ] ; then
  JAVA=$(which java)
fi
if [ -z "$JPS" ] ; then
  JPS=$(which jps)
fi

if [ -z "$JAVA" ] ; then
  echo "Cannot find a Java JDK. Please set either set JAVA or put java (>=1.8) in your PATH." 2>&2
  exit 1
fi

if [ -z "$JPS" ] ; then
  echo "Cannot find a Java Devel. Please install java-devel." 2>&2
  JPS=$(which jps)
fi

psid=0

checkpid() {

   javaps=`$JPS -l | grep $APP_MAINCLASS`

   if [ -n "$javaps" ]; then
      psid=`echo $javaps | awk '{print $1}'`
   else
      psid=0
   fi
}

start() {
   checkpid

   if [ $psid -ne 0 ]; then
      echo "================================"
      echo "warn: $APP_MAINCLASS already started! (pid=$psid)"
      echo "================================"
   else
      echo -n "Starting $APP_MAINCLASS ..."
      JAVA_CMD="$JAVA $JAVA_OPTS -classpath $CLASSPATH $APP_MAINCLASS >/dev/null 2>&1 &"
      echo $JAVA_CMD
      su - $RUNNING_USER -c "$JAVA_CMD"
      checkpid
      if [ $psid -ne 0 ]; then
         echo "(pid=$psid) [OK]"
      else
         echo "[Failed]"
      fi
   fi
}

stop() {
   checkpid

   if [ $psid -ne 0 ]; then
      echo -n "Stopping $APP_MAINCLASS ...(pid=$psid) "
      su - $RUNNING_USER -c "kill -15 $psid"
      if [ $? -eq 0 ]; then
         echo "[OK]"
      else
         echo "[Failed]"
      fi

      #checkpid
      #if [ $psid -ne 0 ]; then
      #   stop
      #fi
   else
      echo "================================"
      echo "warn: $APP_MAINCLASS is not running"
      echo "================================"
   fi
}

status() {
   checkpid

   if [ $psid -ne 0 ];  then
      echo "$APP_MAINCLASS is running! (pid=$psid)"
   else
      echo "$APP_MAINCLASS is not running"
   fi
}

info() {
   echo "System Information:"
   echo "****************************"
   echo `head -n 1 /etc/issue`
   echo `uname -a`
   echo
   echo "JAVA=$JAVA"
   echo `$JAVA -version`
   echo
   echo "APP_HOME=$BASE"
   echo "APP_MAINCLASS=$APP_MAINCLASS"
   echo "****************************"
}

case "$1" in
   'start')
      start
      ;;
   'stop')
     stop
     ;;
   'restart')
     stop
     start
     ;;
   'status')
     status
     ;;
   'info')
     info
     ;;
  *)
     echo "Usage: $0 {start|stop|restart|status|info}"
     exit 1
esac
exit 0