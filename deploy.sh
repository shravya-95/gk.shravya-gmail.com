#!/usr/bin/env bash

########################################################
# Declared Variables
########################################################

counter=0
option=$1
sales_directory=~/salesdb
hdfs_directory=/salesdb
path_to_files=~/hadoop_project

########################################################
# Functions
########################################################

display_help() {
    echo "Usage: $) [option...] " >&2
    echo
    echo "  -h, --help          display help contents"
    echo "  -g, --get_data           get data from url"
    echo "  -l, --load          load data to hdfs"
    echo "  -c1, --create_raw_tables      create tables for deliverable 2 step 1"
    echo "  -c2, --create_trains_tables    create tables for deliverable 2 step 2.3"
    echo "  -cv, --create_trains_view     create views for deliverable 3, step 2.4"
    echo "  -dc, --drop_raw_cascade      drop raw sales database cascade"
    echo "  -d2c, --drop_trains_cascade      drop sales database cascade"
    echo "  -dv, --drop_train_views      drop sales views"
    echo "  -dh, --delete-hdfs       delete all sales data in hdfs"
    exit 1
}


get_data() {
    echo "getting data from https://csci5751-2020sp.s3-us-west-2.amazonaws.com/sales-data/salesdata.tar.gz"
    wget https://csci5751-2020sp.s3-us-west-2.amazonaws.com/sales-data/salesdata.tar.gz
    echo "unzipping train data"
    tar -xvzf salesdata.tar.gz
    mv raw $sales_directory
}


do_hdfs() {
  echo creating hdfs directory $hdfs_directory
  sudo -u hdfs hdfs dfs -mkdir $hdfs_directory

  for file in "$sales_directory"/*
     do
     echo processing "$file"
     filename=$(basename -- "$file")
     echo creating hdfs directory: $hdfs_directory/"${filename%.*}"
     sudo -u hdfs hdfs dfs -mkdir $hdfs_directory/"${filename%.*}"
     echo puting file $trains_directory/$filename to hdfs directory: $hdfs_directory/"${filename%.*}"
     sudo -u hdfs hdfs dfs -put $trains_directory/$filename $hdfs_directory/"${filename%.*}"/

#     echo "$filename"

   done
   echo Changing owner of hdfs directory to hive
   sudo -u hdfs hdfs dfs -chown -R hive:hive $hdfs_directory
}

create_raw() {
   echo creating raw tables on csv files
   impala-shell -f "$path_to_files"/sql/ddl_create_sales_raw.sql

}

create_sales() {
   echo creating parquet tables as Select
   impala-shell -f "$path_to_files"/sql/ddl_create_sales.sql

}

create_sales_views() {
   echo creating sales views on parquet tables
   impala-shell -f "$path_to_files"/sql/create_sales_views.sql
}


drop_raw_database() {
   echo Dropping database and cascade tables
   impala-shell -q "DROP DATABASE IF EXISTS zeros_and_ones_sales_raw CASCADE;"

}

drop_sales_database() {
   echo Dropping database and cascade tables
   impala-shell -q "DROP DATABASE IF EXISTS zeros_and_ones_sales CASCADE;"

}

drop_sales_views() {
    echo Removing sales views
    impala-shell -q "Drop VIEW IF EXISTS zeros_and_ones_sales.<>;"

}

delete_hdfs_raw() {
    echo Removing raw sales data from HDFS
    sudo -u hdfs hdfs dfs -rmr $hdfs_directory
}



########################################################
# Run Time Commands
########################################################

while [ $counter -eq 0 ]; do
    counter=$(( counter + 1 ))

    case $option in
      -h | --help)
          display_help
          ;;

      -g | --get_data)
          echo "Geting data and unzipping file"
          get_data
          ;;

      -l | --load)
          echo "Loading data to HDFS"
          do_hdfs
          ;;

      -c1 | --create_raw_tables)
          echo "Creating raw external tables"
          create_raw
          ;;

      -c2 | --create_trains_tables)
          echo "Creating raw external tables"
          create_trains
          ;;

      -cv | --create_trains_views)
          echo "Creating sales views"
          create_trains_views
          ;;

      -dc | --drop_raw_cascade)
          echo "Dropping DATABASE CASCADE"
          drop_raw_database
          ;;

      -d2c | --drop_trains_cascade)
          echo "Dropping DATABASE CASCADE"
          drop_trains_database
          ;;

      -dv | --drop_trains_views)
          echo "Dropping trains Views"
          drop_trains_views
          ;;

      -dh | --delete_hdfs_raw)
          echo "Removing Data from HDFS"
          delete_hdfs_raw
          ;;

      --) # End of all options
          shift
          break
          ;;

      -*)
          echo "Error: Unknown option: $1" >&2
          ## or call function display_help
          exit 1
          ;;

      *)  # No more options
          break
          ;;

    esac
done