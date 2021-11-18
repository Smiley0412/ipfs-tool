# Import Modules (python3.6)

import csv
import ipfsApi
import  urllib
import json

# Initialize Global variables

inputFileName = 'input/input.csv'
contractAddr = '0x6FCA0F70BcC4a86786c79414F8E84BD542F7c250'
arrMetainfo= []
arrImagesPath = []
arrJsonPath = []
arrMetaLast = []

# Load CSV file and make array for METADATA

def createArrayFromCSV(_inputFile):
    tempMetadata = []
    with open(_inputFile, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file) 
        line_count = 0
        for row in csv_reader:
                temDict = {}
                if line_count == 0:
                    line_count += 1
                
                tempDict = {
                            'id' : row["ID"],
                            'instagram_id' : row["Instagram ID"],
                            'pic_share_url' : row['Instagram Pic Share URL'],
                            'direct_link' : row['Instagram Direct Link'],
                            'image_url' : row['WasabiURL'],
                            'vintage_date' : row['Vintage Date'],
                            'image_path' : '',
                            'image_cid' : '',
                            'image_ipfs_url' : '',
                            'metadata_cid' : '',
                            'metadata_ipfs_url' : '',
                            'json_path' : ''
                           }
                tempMetadata.append(tempDict)
                line_count += 1
        print(f'- Processed {line_count} lines in CSV file')
    return tempMetadata

# Check the length of CSV data

def hasData(_inputFile):
    hasDatas = False
    with open(_inputFile, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file) 
        csv_length = len(list(csv_reader))
        if csv_length > 0:
            hasDatas = True
    return hasDatas

# Download Image From URLS

def downloadImgFromURL(imgUrls):
    tempImagesPath = []
    print("- Downloading Images from URL...")
    for url in imgUrls:
        urllib.request.urlretrieve(url["image_url"], f'images/{url["instagram_id"]}.jpg')
        url["image_path"] = f'images/{url["instagram_id"]}.jpg'
        tempImagesPath.append(f'images/{url["instagram_id"]}.jpg')
        print(f'**{url["instagram_id"]}.jpg is downloaded in folder images ')
    arrMetainfo = imgUrls
    return imgUrls

# Upload Images from Local to IPFS

def uploadImgs(_api, _arrMetainfoUpdated):
    for data in _arrMetainfoUpdated:
       res = _api.add(data["image_path"])
       cid = res[0]['Hash']
       data["image_cid"] = cid
       data["image_ipfs_url"] = f'https://ipfs.io/ipfs/{cid}?filename={cid}'
       print(f'**{data["image_path"]} file is uploaded to IPFS')
       dataUpdated = createJsonFile(cid, data)
       arrMetaLast.append(dataUpdated)

    return arrMetaLast

# Upload METADATA json files into IPFS

def uploadJsonFiles(_api, _jsonData):
    dicForCSV = []
    for row in _jsonData:
        res = _api.add(row["json_path"])
        cid = res[0]['Hash']
        row['metadata_cid'] = cid
        row['metadata_ipfs_url'] = f'https://ipfs.io/ipfs/{cid}?filename={cid}'
        dicForCSV.append(row)
        print(f'**{row["json_path"]} file is uploaded to IPFS')
        
    print("- All metadata json files are uploaded")
    return dicForCSV

# Update Output File in Output folder

def updateCSVFile(_metadata): 
    csv_headers = ["ID", "Instagram Username", "Instagram Pic Share URL", "Instagram Direct Link", "WasabiURL", "Image IPFS CID", "Image IPFS URL", "Metadata CID", "Metadata IPFS URL", "Contract Address", "Token ID"]
    csv_rowData = []
    for row in _metadata:
        tempRowData = {
            "ID" : row["id"],
            "Instagram Username" : row["instagram_id"],
            "Instagram Pic Share URL" : row["pic_share_url"],
            "Instagram Direct Link" : row["direct_link"],
            "WasabiURL" : row["image_url"],
            "Image IPFS CID" : row["image_cid"],
            "Image IPFS URL" : row["image_ipfs_url"],
            "Metadata CID" : row["metadata_cid"],
            "Metadata IPFS URL" : row["metadata_ipfs_url"],
            "Contract Address" : contractAddr,
            "Token ID" : row["id"]
        }
        csv_rowData.append(tempRowData)
    print("- Updating metadata.csv file...")
    with open('output/metadata.csv', 'w', encoding='UTF8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=csv_headers)
        writer.writeheader()
        writer.writerows(csv_rowData)
    print("- Updated metadata.csv file in folder output ")

# Create Metadata.json files in metadata folder

def createJsonFile(_cid, _data):
    
    dataForJsonFile = {
        "name": "$NAME",
        "description": "$DESCRIPTION",
        "external_url": _data["image_url"],
        "instagram_username": _data["instagram_id"],
        "instagram_share_url": _data["pic_share_url"],
        "instagram_direct_link": _data["direct_link"],
        "image_ipfs_cid": _cid,
        "contract_address": contractAddr,
        "vintage_date": _data["vintage_date"],
        "token_id": int(_data["id"])
    }
   
    with open(f"metadata/{_data['instagram_id']}.json", "w") as outfile:
        json.dump(dataForJsonFile, outfile)
        _data["json_path"] = f"metadata/{_data['instagram_id']}.json"
        print(f"**metadata/{_data['instagram_id']}.json file is created in folder metadata ")
    print('- All Metadata json files are created!')
    return _data

# Main Function

if __name__ == '__main__':
    #Connect to local node
    try:
        api = ipfsApi.Client('127.0.0.1', 5001)
        if hasData(inputFileName) == True:
            arrMetainfo = createArrayFromCSV(inputFileName)
            arrMetainfoUpdated = downloadImgFromURL(arrMetainfo)
            arrMetaFinal = uploadImgs(api, arrMetainfoUpdated)
            arrMetaForCSV = uploadJsonFiles(api, arrMetaFinal)
            updateCSVFile(arrMetaForCSV)
        else :
            print(f"There is no Datas in {inputFileName} file")
    except ipfsApi.exceptions.ConnectionError as ce:
        print('Connection failed with IPFS server')




