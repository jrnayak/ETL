# Introduction 

This project takes data from a directory named 'input', performs ETL and produces processed data to a directory named 'output'. 
The degree of parallelism n is given on the command line as an option to the program (-p) and defaults to 3.

# Getting Started
1.	Download the zipped tar file to a path
    unzip and untar the file
    
    unzip jnayak.tar.gz.zip
    tar xvzf jnayak.tar.gz

2.  change directory to onfido

    `cd onfido`
    
2.  install the required dependencies

    `pip3 install -r analytics/requirements.txt`
3.	optional: There is an option to create an egg file and run the program using setup.py

    python analytics/setup.py bdist_egg
    
# Build and Test
1. from the project directory, Run the script with optional -p (no. of partitions) -config (/path/of/config/file)
 which will call the driver program of the application

`bash run.sh -p 4 -c /home/user/onfido/config/onfido.cfg`

or

`bash run.sh -c /home/user/onfido/config/onfido.cfg`


