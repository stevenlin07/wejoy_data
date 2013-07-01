#!/bin/sh
# author by liyuan7. 2012-12-13
#purpose auto create table, auto,auto worth TED

######conf area start #######
#db count
dbcount=32
dbname=meta_message_
createtablefile=meta_message.sql

host=10.55.45.232
port=3306
user=meyou_test
pass=meyou_test
######conf area start #######

createtablesql=`cat $createtablefile`

for dbnumber in `seq 1 $dbcount` ;do

dropdb="drop database if exists $dbname$dbnumber;";
createdb="create database $dbname$dbnumber;";
userdb="use $dbname$dbnumber;"

##no echo -e here, so stupid
createsql="$dropdb\n$createdb\n$userdb\n$createtablesql\n"

echo "$createsql"
mysql -h"$host" -P"$port" -u"$user" -p"$pass" -e "$createsql"

done;

