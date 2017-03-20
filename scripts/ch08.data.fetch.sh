#!/bin/sh

ROOT_DIR=`pwd`
# ROOT_DIR=~/workspace/aaspark
DATA_DIR=$ROOT_DIR/data
TMP_DIR=/tmp/aaspark

mkdir -p $DATA_DIR/ch08

mkdir -p $TMP_DIR
cd $TMP_DIR/
curl -o nyc-borough-boundaries-polygon.geojson http://nycdatastables.s3.amazonaws.com/2013-08-19T18:15:35.172Z/nyc-borough-boundaries-polygon.geojson
mv nyc-borough-boundaries-polygon.geojson $DATA_DIR/ch08/nyc-boroughs.geojson

curl -o trip_data.7z https://archive.org/download/nycTaxiTripData2013/trip_data.7z

7unzip trip_data.7z
mv trip_data/* $DATA_DIR/ch08/
