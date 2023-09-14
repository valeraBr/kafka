#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
SIGNAL=${SIGNAL:-TERM}

if [[ "$1" == "--process.roles" ]]; then
    ProcessRole="$2"
fi

OSNAME=$(uname -s)
if [[ "$OSNAME" == "OS/390" ]]; then
    if [ -z $JOBNAME ]; then
        JOBNAME="KAFKSTRT"
    fi
    PIDS=$(ps -A -o pid,jobname,comm | grep -i $JOBNAME | grep java | grep -v grep | awk '{print $1}')
elif [[ "$OSNAME" == "OS400" ]]; then
    PIDS=$(ps -Af | grep -i 'kafka\.Kafka' | grep java | grep -v grep | awk '{print $2}')
else
    PIDS=$(ps ax | grep ' kafka\.Kafka ' | grep java | grep -v grep | awk '{print $1}'| xargs)
    ConfigFiles=$(ps ax | grep ' kafka\.Kafka ' | grep java | grep -v grep | sed 's/--override property=[^ ]*//g' | awk 'NF>1{print $NF}' | xargs)
fi

if [ -z "$PIDS" ]; then
  echo "No kafka server to stop"
  exit 1
else
  if [ -z "$ProcessRole" ]; then
    kill -s $SIGNAL $PIDS
  else
    IFS=' ' read -ra ConfigFilesArray <<< "$ConfigFiles"
    IFS=' ' read -ra PIDSArray <<< "$PIDS"
    keyword="process.roles="
    for ((i = 0; i < ${#ConfigFilesArray[@]}; i++)); do
        PRCRole=$(sed -n "/$keyword/ { s/$keyword//p; q; }" "${ConfigFilesArray[i]}")
        if [ "$PRCRole" == "$ProcessRole" ]; then
          kill -s $SIGNAL ${PIDSArray[i]}
#          echo "Killed ${PRCRole} with PID ${PIDSArray[i]}"
#        else
#          echo "Skip killing ${PRCRole} with PID ${PIDSArray[i]}"
        fi
    done
  fi
fi
