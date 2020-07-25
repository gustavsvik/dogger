#!/bin/bash

sleep 120

#Cycle eth0 interface if not connected
logger "Checking if interface eth0 not connected..."
if ! (ethtool eth0 | egrep "Link.*yes" && ifconfig eth0 | grep "inet") ; then
  logger 'Cycling interface...'
  ifconfig eth0 down
  sleep 10
  ifconfig eth0 up
  sleep 30
else
  logger 'Interface eth0 is connected...'
fi

# Disable WiFi if wired.
logger "Checking Network interfaces..."
if (ethtool eth0 | egrep "Link.*yes" && ifconfig eth0 | grep "inet") ; then
  logger 'Disabling WiFi...'
  ifconfig wlan0 down
else
  logger 'WiFi is still enabled: Ethernet is down or ethtool is not installed.'
fi
