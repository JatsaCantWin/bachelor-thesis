#!/bin/sh
sudo docker run --volume "$(pwd)":/thesis-template/ mbredel/thesis-template && docker rm $(docker ps -lq)
