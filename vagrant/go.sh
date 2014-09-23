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

#!/bin/sh -Eux

#  Trap non-normal exit signals: 1/HUP, 2/INT, 3/QUIT, 15/TERM, ERR
trap founderror 1 2 3 15 ERR

founderror()
{
        exit 1
}

exitscript()
{
        #remove lock file
        #rm $lockfile
        exit 0
}

apt-get install -y git
apt-get install -y mercurial

wget https://storage.googleapis.com/golang/go1.3.1.linux-amd64.tar.gz
tar -C /usr/local -xzf go1.3.1.linux-amd64.tar.gz
rm go1.3.1.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin
export GOPATH=/usr/local/go/home/

go get github.com/stealthly/go-kafka

#GO_BROKER=$(sudo grep -R go-broker /etc/hosts)
#if [ -z "$GO_BROKER" ]; then
#    echo "adding go-broker to /etc/hosts.."
echo "192.168.86.10 go-broker" >> /etc/hosts
#fi

#GO_ZOOKEEPER=$(sudo grep -R go-zookeeper /etc/hosts)
#if [ -z "$GO_ZOOKEEPER" ]; then
#    echo "adding go-zookeeper to /etc/hosts.."
echo "192.168.86.5 go-zookeeper" >> /etc/hosts
#fi

/opt/apache/kafka/bin/kafka-topics.sh --create --zookeeper go-zookeeper:2181 --replication-factor 1 --partitions 2 --topic partitions_test

exitscript