# folder structure setup
# bash shell script
# run once in project directory upon cloning repo

mkdir -p data rates code figures

cd data
mkdir -p raw-data subprocessed-data processed-data
cd ../code
mkdir -p data-processing data-analysis data-visualization
cd data-processing
mkdir raw-data-processing rate-calculation rate-conversion
cd ../..

#verify folders have been set up properly
ls -R
