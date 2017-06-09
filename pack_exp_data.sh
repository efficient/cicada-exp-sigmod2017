#!/bin/sh

DATE=`date -u +%Y%m%d_%H%M%S`

tar cvjf ../exp_data_$DATE.tar.bz2 --numeric-owner $1

