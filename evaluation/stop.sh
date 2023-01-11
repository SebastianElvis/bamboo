#!/bin/bash

USERNAME="ec2-user"
SERVER_ADDR=(`cat public_ips.txt`)

j=0
for addr in ${SERVER_ADDR[@]}
do
    let j+=1
    ssh -oStrictHostKeyChecking=no -i ~/.ssh/ebft.pem -t $USERNAME@$addr "echo ---- "successfully pkill client/server and clear logs on node ${j}" --- ; sudo pkill server ; sudo pkill client ; rm -f /home/$USERNAME/bamboo/server.* ; rm -f /home/$USERNAME/bamboo/client.*"; sleep 0.01 &
done
wait