#!/bin/sh

# default 50MB
declare -i NEED_FOR_MEM_SIZE=50

# create disk
hdiutil attach -nomount "ram://$((2 * 1024 * NEED_FOR_MEM_SIZE))"
