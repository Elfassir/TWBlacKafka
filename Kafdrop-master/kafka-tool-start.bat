@echo off 
set /p hostname=Please enter the Zookeeper host  : 
set /p port=Please enter the Zookeeper port : 
Java -jar kafdrop-2.0.0.jar --zookeeper.connect=%hostname%:%port%
