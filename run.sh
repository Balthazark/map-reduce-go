#!/bin/bash

# Function to connect and start workers on a given instance
start_workers() {
  local instance_ip=$1
  ssh -i "./map-reduce.pem" ubuntu@"$instance_ip" << EOF
    cd ../../mnt/efs/fs1/map-reduce-go/src/main
    echo 'Build complete, Starting workers'
    for _ in {1..5}; do
      sudo go run mrworker.go wc.so &
    done
    wait # Wait for all background processes to finish
EOF
}

# Connect to the coordinator instance and start coordinator in the background
ssh -i "./map-reduce.pem" ubuntu@ec2-18-204-6-133.compute-1.amazonaws.com << EOF
  cd ../../mnt/efs/fs1/map-reduce-go/src/main
  sudo rm mr-*
  echo 'Build complete, Starting coordinator'
  sudo go run -race mrcoordinator.go pg-*.txt &
  coordinator_pid=\$!
  wait \$coordinator_pid # Wait for the coordinator to finis
EOF

# Start workers on each worker instance
start_workers "ec2-54-82-221-152.compute-1.amazonaws.com" | tee worker_instance_1_output.txt & 
start_workers "ec2-54-160-107-37.compute-1.amazonaws.com" | tee worker_instance_2_output.txt &
# Add more worker instances as needed...

# Wait for all background processes to finish
wait
