# Ingestion of metadata in B2FIND
This document describes how you can publish metadata records in the B2FIND catalogue.
We will take you through the steps from harvesting from B2SHARE, over mapping to teh B2FIND schema up to uploading into the B2FIND portal.

##Prerequisites
1. The python script `b2find_ingest.py` comes with this git repos 
2. A dataset uploaded to the B2SHARE training instance
3. ...

## Harvesting

You can use the python script `b2find_ingest.py` in the mode `h` to harvest directly from command line.

For this part we will specify as source the OAI provider URL of the B2SHARE training instance by the option `-s SOURCE`. As `verb` we let the default methode `ListRecords` and specifiy as metadata format (option `--mdprefix`) `marcxml`. The community we name `b2share` :


```sh
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
