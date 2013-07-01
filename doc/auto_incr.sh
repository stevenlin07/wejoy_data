#!/bin/sh
# author by liyuan7.create at 2012-10-29, update at 2012-12-13
#purpose auto create table, auto,auto worth TED


######conf area start #######
#year which to generate
year=13
#month witch to generate, if current month is  one-digit, like 2, then month should be 02
month=02

#db count
dbcount=32

host=172.16.89.69
port=3306
user=weimi_test
pass=weimi_test
######conf area start #######


day=1
daysofmonth=`cal | xargs | awk '{print $NF}'`
#daysofmonth=2


get_day(){
 newday=`echo $1| awk '{if(length($1)<2) print "0"$1 ;else print $1}'`
 echo $newday
}

while [ $day -le $daysofmonth ]
do

  for count in `seq 1 $dbcount` ;do

usedb="use meta_message_$count;"

tablename="meta_message_$year$month`get_day $day`"
droptable="drop table if exists $tablename;"
createtablesql="CREATE TABLE $tablename(
 \`aid\` int(10) unsigned NOT NULL AUTO_INCREMENT,
  \`id\` varchar(48) NOT NULL,
  \`meta\` blob NOT NULL,
  PRIMARY KEY (\`aid\`),
  UNIQUE KEY udx_id (\`id\`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;"

createsql=`echo -e "$usedb\n$droptable\n$createtablesql\n"`
echo $createsql

mysql -h"$host" -P"$port" -u"$user" -p"$pass"  -e "$createsql"

done;

day=$(($day+1))

  done;
