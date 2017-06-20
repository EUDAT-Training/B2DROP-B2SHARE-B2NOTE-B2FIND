# Ingestion of metadata in B2FIND
This document describes how you can publish metadata records in the B2FIND catalogue.
We will take you through the steps from harvesting from B2SHARE, over mapping to teh B2FIND schema up to uploading into the B2FIND portal.

##Prerequisites
1. The python script `b2find_ingest.py` comes with this git repos 
2. A dataset uploaded to the B2SHARE training instance or/and an OAI-endpoint with harvestable records available
3. ...

## Harvesting

You can use the python script `b2find_ingest.py` in the mode `h` to harvest directly from command line.

As `verb` we let the default methode `ListRecords` 

### Example 1 : Harvest from (the OAI endpoint) of B2SHARE

```sh
The community (option `-c`) we name `b2share` and specify as source (option `-s`) the OAI provider URL of the B2SHARE training instance. As metadata format (option `--mdprefix`) we take the `marcxml`, the metadata format used preferably by B2SHARE to exchange metadata.  

./b2find_ingest.py --mode h -c b2share -s https://trng-b2share.eudat.eu/api/oai2d --mdprefix marcxml

```

If all works fine you should find the harvested XML files in the directory

```sh
oaidata/b2share-marcxml/SET/xml/
```
 
## Mapping

You can use the python script `b2find_ingest.py` in the mode `m` to map the harvested XML files to JSON files formated in the B2FIND schema. All other options can be choosen as above.


```sh
./b2find_ingest.py --mode m -c b2share -s https://trng-b2share.eudat.eu/api/oai2d --mdprefix marcxml

```

If all works fine you should find the mapped JSON files in the directory

```sh
oaidata/b2share-marcxml/SET/json/
```
 
## Uploading

You can use the python script `b2find_ingest.py` in the mode `u` to uplaod the mapped JSON files to the B2FIND catalogue and portal. Beside the options used above you ahve to specify the address of B2FIND portal (option `-i`) and a valid API key (option '--api-key). 


```sh
./b2find_ingest.py --mode u -c b2share -s https://trng-b2share.eudat.eu/api/oai2d --mdprefix marcxml -i trng-b2find.dkrz.de --auth <API-KEY>
```

If all works fine you should find the uploaded records at

```sh
http://trng-b2find.eudat.eu/groups/b2share
```

.... 

## Ingestion in one go


```sh
./b2find_ingest.py -c rda -s http://rda-summerschool.csc.fi/repository/oai --mdprefix oai_dc -i trng-b2find.dkrz.de --auth <API-KEY>
```



http://rda-summerschool.csc.fi/repository/oai/request?verb=ListRecords&metadataPrefix=oai_dc&set=hdl_123456789_3