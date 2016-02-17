import json
import os
import sys

from multiprocessing import Pool
from random import seed
from table import Table
from tableDumper import TableDumper

def buildTable(tableConfig):
    table = Table(tableConfig['name'], tableConfig['rows'], tableConfig['columns'], tableConfig['stringsForEachInt'], tableConfig['stringLength'], tableConfig['uniqueValues'], outputDirectory, metaDataFile)
    memoryBudget = table.build()
    print "Finished building table: %s" % (tableConfig['name'])

    return memoryBudget

def buildTables(configFile):
    with open(configFile) as configFile:
        config = json.load(configFile)

    tableConfigs = config['tables']

    # For testing purposes, uncomment for random tables
    seed(1238585430324)

    if config['multiThreaded']:
        threadPool = Pool(len(tableConfigs))

        memoryBudgets = threadPool.map(buildTable, tableConfigs, 1)
        overallMemoryBudget = reduce(lambda x, y: x + y, memoryBudgets)
    else:
        overallMemoryBudget = 0

        for tableConfig in tableConfigs:
            overallMemoryBudget += buildTable(tableConfig)

    metaDataFile.write("Overall memory budget for indexation of all tables: %i\n" % (overallMemoryBudget))
    metaDataFile.close()

    if config['binary']:
        for tableConfig in tableConfigs:
            TableDumper("%s/%s.tbl" % (outputDirectory, tableConfig['name']))

if len(sys.argv) <> 3:
    print "Usage: python generator.py config.json outputDirectory"
    sys.exit()
else:
    configFile = sys.argv[1]
    outputDirectory = sys.argv[2]

    if not os.path.exists(outputDirectory):
        os.makedirs(outputDirectory)

    metaDataFile = open("%s/metadata" % (outputDirectory), "w")
    buildTables(configFile)