#!/bin/bash
mkdir -p /tmp/scala_jar
hadoop fs -copyToLocal s3://eel-fall-2014/pfga/scala-library-2.10.4.jar /tmp/scala_jar
sudo  mv /tmp/scala_jar/scala-library-2.10.4.jar /home/hadoop/lib/
