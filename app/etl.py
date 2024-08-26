import pandas as pd
import logging
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)
dataFile = "Online_Retail_Data_Set.csv"
dataTypes = {
    "InvoiceNo": "float64",
    "StockCode": "string",
    "Description": "string",
    "Quantity": "float64",
    "InvoiceDate": "datetime",
    "UnitPrice": "float64",
    "CustomerID": "float64",
    "Country": "string"
}

def main():
    logging.basicConfig(filename='etl.log', level=logging.INFO)
    logger.info("Starting")
    df = loadData(f'{dataFile}')
    cleanedDF, removedDF = cleanData(df)
    cleanedTransformedDF = transformData(cleanedDF,dataTypes)
    saveDataToFile(cleanedTransformedDF, "cleanedData.csv")
    saveDataToFile(removedDF, "removedData.csv")
    logger.info("Finished")

def loadData(filepath):
    logger.info(f"loadData - {filepath} starting")
    df = pd.read_csv(f'input/{filepath}', encoding = "ISO-8859-1")
    logger.info(f"loadData - first rows:\n{df.head()}")
    logger.info(f"loadData - columns info:\n{df.dtypes}")
    logger.info(f"loadData - finished")
    return df

def cleanData(dataframe):
    logger.info("cleanData - starting")
    mandatoryColumns = ["InvoiceNo","StockCode","Quantity","UnitPrice","CustomerID"]
    uniqueFields = ["InvoiceNo","StockCode","Quantity","UnitPrice","CustomerID"]
    cleanedDF = dataframe
    removedDF = pd.DataFrame().reindex(columns=dataframe.columns)
    removedFrames = [removedDF]

    # Removing invalid transactions with Quantity or UnitPrice less or equal to 0
    if cleanedDF[cleanedDF.Quantity <= 0].shape[0]>0:
        logger.info(f"cleanData - Removing {cleanedDF[cleanedDF.Quantity <= 0].shape[0]} rows with 0 or negative quantity")
        removedFrame = cleanedDF[cleanedDF.Quantity <= 0]
        removedFrame["ErrorCode"] = 1
        removedFrames.append(removedFrame)
        cleanedDF = cleanedDF[cleanedDF.Quantity > 0]

    if cleanedDF[cleanedDF.UnitPrice<=0].shape[0]>0:
        logger.info(f"cleanData - Removing {cleanedDF[cleanedDF.UnitPrice<=0].shape[0]} rows with 0 or negative unit price")
        removedFrame = cleanedDF[cleanedDF.UnitPrice <= 0]
        removedFrame["ErrorCode"] = 1
        removedFrames.append(removedFrame)
        cleanedDF = cleanedDF[cleanedDF.UnitPrice > 0]
    
    # Remove rows with N/A values for mandatory fields
    for column in mandatoryColumns:
        if cleanedDF[column].isna().sum()>0:
            logger.info(f"cleanData - Removing {cleanedDF[column].isna().sum()} rows with invalid value for {column} column")
            removedFrame = cleanedDF[cleanedDF[column].isna() == True]
            removedFrame["ErrorCode"] = 2
            removedFrames.append(removedFrame)
            cleanedDF = cleanedDF[cleanedDF[column].isna()==False]

    # Removing duplicate rows according to primary key
    if cleanedDF[cleanedDF.duplicated(subset=uniqueFields)].shape[0] > 0:
        logger.info(f"cleanData - Removing {cleanedDF[cleanedDF.duplicated(subset=uniqueFields)].shape[0]} duplicated rows according to uniqueness {uniqueFields}")
        removedFrame = cleanedDF[cleanedDF.duplicated(subset=uniqueFields)]
        removedFrame["ErrorCode"] = 3
        removedFrames.append(removedFrame)
        cleanedDF = cleanedDF[-cleanedDF.duplicated(subset=uniqueFields)]

    removedDF = pd.concat(removedFrames).reset_index(drop=True)
    logger.info("cleanData - finished")
    return cleanedDF, removedDF

def transformData(dataframe, columnTypes):
    for column in columnTypes:
        if columnTypes[column] != dataframe[column].dtype:
            logger.info(f"transformData - converting column {column} from {dataframe[column].dtype} to {columnTypes[column]}")
            if columnTypes[column]=="datetime":
                dataframe[column] = pd.to_datetime(dataframe[column], format="%d-%m-%Y %H:%M")
            else:
                dataframe[column] = dataframe[column].astype(columnTypes[column])

    return dataframe

def saveDataToFile(frame, filename):
    logger.info(f"saveDataToFile - Saving data to {filename}")
    frame.to_csv(f'output/{filename}')

if __name__ == "__main__":
    main()