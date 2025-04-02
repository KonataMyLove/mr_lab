#!/bin/bash

# echo "Starting server..."
./master being_ernest dorian_gray &

sleep 1

echo "Starting client..."
./worker &

# 遇到socket已被占用的情况
# sudo netstat -ltnp
# kill -9 <PID>