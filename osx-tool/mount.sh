#!/bin/sh

# format & mount disk
diskutil eraseVolume HFS+ $1 $2
