go-kafka
========

Quick up and running using Go for Apache Kafka

Use Vagrant to get up and running.

1) Install Vagrant [http://www.vagrantup.com/](http://www.vagrantup.com/)  
2) Install Virtual Box [https://www.virtualbox.org/](https://www.virtualbox.org/)  

In the main kafka folder  

1) vagrant up   
2) vagrant ssh brokerOne   
3) cd /vagrant   
4) sudo ./test.sh

once this is done 
* Zookeeper will be running 192.168.86.5
* Broker 1 on 192.168.86.10
* All the tests in github.com/stealthly/go-kafka/test/* should pass  

You can access the brokers and zookeeper by their IP from your local without having to go into vm.

e.g.

bin/kafka-console-producer.sh --broker-list 192.168.86.10:9092 --topic *get this from the random topic created in test*

bin/kafka-console-consumer.sh --zookeeper 192.168.86.5:2181 --topic *get this from the random topic created in test* --from-beginning
