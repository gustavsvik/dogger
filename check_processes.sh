#!/bin/bash

process_info_token="bash" 
base_dir_path="/var/tmp/"
sub_dir_name=""
process_file_name="processes.txt"

if ! [ -z "$1" ]; then
  process_info_token=$1
fi
if ! [ -z "$2" ]; then
  base_dir_path=$2
fi
if ! [ -z "$3" ]; then
  sub_dir_name=$3
fi
if ! [ -z "$4" ]; then
  process_file_name=$4
fi

sub_dir=$base_dir_path
sub_dir+=$sub_dir_name
sub_dir_path=$sub_dir
if ! [ -z "$3" ]; then
  sub_dir_path+="/"
fi

process_file=$base_dir_path
process_file+=$process_file_name

missing_process_file=$base_dir_path
missing_process_file+="not_running."
missing_process_file+=$process_file_name

missing_process_script=$base_dir_path
missing_process_script+="not_running."
missing_process_script+=$process_file_name
missing_process_script+=".sh"

while true

  do 

    diff \
    <(pgrep -a $process_info_token | grep -e $sub_dir_path | sort -k 3 | cut -d' ' -f3) \
    <(cat $process_file | sort -k 2 | cut -d' ' -f2) \
    | grep -e ">" | grep -e $sub_dir | cut -d' ' -f2 \
    > $missing_process_file

    grep -Fwf $missing_process_file $process_file > $missing_process_script

    $missing_process_script

    sleep 300

  done
