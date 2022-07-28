#!/bin/sh
exec /usr/sbin/sshd -D -p 2022 -e "$@"
