#!/bin/bash

USERNAME="ec2-user"
MAXPEERNUM=(`wc -l public_ips.txt | awk '{ print $1 }'`)

SERVER_ADDR=(`cat public_ips.txt`)
for (( j=1; j<=$MAXPEERNUM; j++))
do
  ssh -oStrictHostKeyChecking=no -i ~/.ssh/ebft.pem -t $USERNAME@${SERVER_ADDR[j-1]} "cd /home/$USERNAME/bamboo ; nohup bash ./run.sh $j"; sleep 0.01 &
  echo replica $j is launched!
done
wait

echo all replicas have launched!

ssh -oStrictHostKeyChecking=no -i ~/.ssh/ebft.pem -t $USERNAME@${SERVER_ADDR[0]} "cd /home/$USERNAME/bamboo ; nohup bash ./runClient.sh"
echo client has been launched!