#!/bin/bash


if [ $# -ne 3 ];then
    echo "exe   cfg    sample_in   sample_out"
    echo ""
    exit -1
fi


set +e
cd `dirname $0`


jarfile=../target/ml.fe-0.0.1-SNAPSHOT-jar-with-dependencies.jar

#java -cp $jarfile org.felix.ml.fe.normalize.reformat.Process $@
java -cp $jarfile org.felix.ml.fe.reformat.Process $@

#exit -1

#===== 

temp_in=./temp_in
sort_in=./sort_in
sort_out=./sort_out
sort_cfg=./sort_cfg
result=./result


head -n1 $3 | sed  "s/\s/\n/g" | sed -n '2,$p' | sort >$sort_out

head -n1 $2 | sed  "s/\s/\n/g" | sed -n '2,$p'>$temp_in 

python ./get_in.py $temp_in   $sort_out | sort >$sort_in

python ./get_cfg.py $1  $sort_out >$sort_cfg

python ./get_join.py $sort_in $sort_out  $sort_cfg >$result

rm -f $temp_in
rm -f $sort_in
rm -f $sort_out
rm -f $sort_cfg










