#!/bin/bash

pkill -9 -f "kvstore_server"
rm -r db_* *.log *.config