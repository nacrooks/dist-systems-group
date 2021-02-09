# Natacha Crooks - ncrooks@berkeley.edu - 2020

# This script describes automatically running a set of experiments.
# Each property file describes a specific experiment in the associated
# configuration folder. Each property file should be a JSON file.

# For simplicity, this experiment framework currently assumes two
# configuration files: a deployment configuration file (for EC2)
# and several experiments files.

# Each experiment here will share the same EC2 setup: machines will
# be created at the start of the experiment and deleted at the end

import shieldExperiment

data = list()

propertyFiles = [
'config/tpcc/tpc_10_noram_server.json',
'config/tpcc/tpc_10_oram_server.json',
'config/tpcc/tpc_10_noram_geoserver.json',
'config/tpcc/tpc_10_oram_geoserver.json',
'config/tpcc/tpc_10_noram_mysql.json'
]

ec2File = "config/tpcc/ec2.json"

# Ensures that all previous EC2 deployments are killed
shieldExperiment.cleanupEc2(ec2File)
# Sets up new EC2 deployment (starts machines, collects IP addresses)
shieldExperiment.setupEc2(ec2File)
for prop in propertyFiles:
    # Updates experiment property file with the new IP addresses
    shieldExperiment.setupConfigForEc2(prop, ec2File)
    # Setups experiments on necessary machines. This includes
    # creating the appropriate folders, setting up the appropriate
    # binaries
    localPath = shieldExperiment.setup(prop, ec2File)
    # Runs experiment (This might include multiple runs of the same
    # experiment if the config files specifies different numbers of clients)
    shieldExperiment.run(prop,False, ec2File)
    # Cleans up remote machines (removes data and kills running processes)
    shieldExperiment.cleanup(prop, ec2File)
    # Calculates results: for each client, computes throughput/latency
    shieldExperiment.calculateParallel(prop, localPath)
# Shuts down EC2 machines
shieldExperiment.cleanupEc2(ec2File)
