#!/bin/bash

rm events.log pipes.log

export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$(realpath ./lib64)"
LD_PRELOAD=$(realpath ./lib64/libruntime.so) ./pa2 "$@"
