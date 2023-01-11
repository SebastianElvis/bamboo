#!/bin/bash

distribute(){
    SERVER_ADDR=(`cat public_ips.txt`)
    for (( j=1; j<=$1; j++))
    do 
        echo -e "---- upload replica ${j}: $2@${SERVER_ADDR[j-1]} \n ----"
        rsync -Pav -e "ssh -o StrictHostKeyChecking=no -i $HOME/.ssh/ebft.pem" ./config.json $2@${SERVER_ADDR[j-1]}:/home/ec2-user/bamboo/config.json &
        rsync -Pav -e "ssh -o StrictHostKeyChecking=no -i $HOME/.ssh/ebft.pem" ./run.sh $2@${SERVER_ADDR[j-1]}:/home/ec2-user/bamboo/run.sh &
    done
    wait
}

USERNAME="ec2-user"
MAXPEERNUM=(`wc -l public_ips.txt | awk '{ print $1 }'`)

# distribute files
distribute $MAXPEERNUM $USERNAME
