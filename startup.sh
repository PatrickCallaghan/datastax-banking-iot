echo 'Installing Maven'
if [ -f /etc/redhat-release ]; then
  yum install maven -y
fi

if [ -f /etc/lsb-release ]; then
  apt-get install maven -y
fi

echo 'Building schema'
mvn clean compile exec:java -Dexec.mainClass="com.datastax.demo.SchemaSetup" -DcontactPoints=node0

echo 'Creating core'
dsetool create_core datastax_banking_iot.latest_transactions generateResources=true reindex=true coreOptions=rt.yaml

echo 'Starting load data -> loader.log'
nohup mvn exec:java -Dexec.mainClass="com.datastax.banking.Main" -DnoOfTransactions=10000000 -DnoOfCreditCards=1000000 -DcontactPoints=node0 > loader.log &

echo 'Starting web server on port 8081 -> jetty.log'
nohup mvn jetty:run -DcontactPoints=node0 -Djetty.port=8081 > jetty.log & 

sleep 2

echo 'Finished setting up'
