#!/bin/sh
sudo docker run --volume "$(pwd)":/thesis-template/ mbredel/thesis-template && sudo docker rm $(sudo docker ps -lq)
