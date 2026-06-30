#!/bin/sh

ssh -N -D 12347 -L 3306:wikidatawiki.analytics.db.svc.wikimedia.cloud:3306 \
  -o ServerAliveInterval=30 \
  -o ServerAliveCountMax=6 \
  -o TCPKeepAlive=yes \
  -o ExitOnForwardFailure=yes \
  -o ServerAliveInterval=30 \
  bovlb@login.toolforge.org