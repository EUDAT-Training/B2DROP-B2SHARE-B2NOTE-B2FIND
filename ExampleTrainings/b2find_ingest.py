#!/usr/bin/env python

"""b2find_ingest.py
  Ingestion of metadata in B2FIND

Copyright (c) 2016 Heinrich Widmann (DKRZ) Licensed under AGPLv3.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""

import optparse
import os, sys, io, time, datetime
from collections import OrderedDict
# For HARVESTER
import sickle as SickleClass
import lxml.etree as etree
import uuid
# For MAPPER
import simplejson as json
import re, codecs 
import iso639
import Levenshtein as lvs
# For UPLOADER
from urllib import quote
from urllib2 import Request, urlopen
from urllib2 import HTTPError,URLError

class HARVESTER():
    
    """
    # HARVESTER provides methods to harvest metadata records via OAI-PMH
    #
    """
    
    def __init__ (self, base_outdir):
        self.base_outdir = base_outdir

    def harvest(self, community, source, verb, mdprefix, subset):
        # harvest (HARVESTER object, community, source, verb, mdprefix, subset)
        # harvests all files with <mdprefix> and <subset> from <source> 
        # via sickle module and store those to hard drive.
        #
        # Return Value:
        # --------------
        # status dictionary    

        stats = {
            "count"    : 0,    # number of all provided datasets per subset
            "scount"     : 0,    # number of all successful harvested datasets per subset
            "ecount"    : 0,    # number of all failed datasets per subset
            "dcount"    : 0,    # number of all deleted datasets per subset
            "timestart" : time.time(),  # start time per subset process
        }

        # sickle instance
        sickle = SickleClass.Sickle(source, max_retries=3, timeout=300)

        try:
            records = sickle.ListRecords(metadataPrefix=mdprefix,set=subset)
        except Exception, e:
            print "[ERROR] %s" % e
            return -1

        if not subset : subset='SET'
        subsetdir = '%s/%s-%s/%s' % (self.base_outdir,community,mdprefix,subset)
        if (not os.path.isdir(subsetdir+'/xml')):
            os.makedirs(subsetdir+'/xml')

        print ' |-Output path\t %s/xml' % subsetdir 
        print '    |   | %-4s | %-20s | %-45s |\n    ---------------------------------------------------------------' % ('#','OAI Identifier','File Identifier')
        for record in records:
            stats['count']+=1
            oai_id = record.header.identifier
            # generate a uniquely identifier for this dataset:
            uid = str(uuid.uuid5(uuid.NAMESPACE_DNS, oai_id.encode('ascii','replace')))
                
            xmlfile = '%s/xml/%s.xml' % (subsetdir,os.path.basename(uid))
            try:
                print '    | h | %-4d | %-20s | %-45s |' % (stats['count'],oai_id,uid)               
                # get the raw xml content:    
                metadata = etree.fromstring(record.raw)
                if (metadata is not None):
                    metadata = etree.tostring(metadata, pretty_print = True).encode('ascii', 'ignore') 
                    # write metadata in file:
                    try:
                        with open(xmlfile, 'w') as f:
                            f.write(metadata)
                    except IOError, e:
                        stats['ecount'] +=1
                        print "[ERROR %s] Cannot write metadata to xml file %s" % (xmlfile,e)
                        continue
                        
                else:
                        stats['ecount'] += 1
                        print '[WARNING]  No metadata available for %s' % oai_id
            except (TypeError,Exception) as e:
                print ' [ERROR %s] during encoding of record %s' % (e,oai_id)
                stats['ecount']+=1        
                continue

            stats['scount']+=1        
                           
        return stats

class MAPPER():

    """
    # MAPPER provides methods to convert harvested XML files to JSON records formated in B2FIND schema
    """

    def __init__ (self,base_outdir):
        self.base_outdir = base_outdir
        # Read in B2FIND metadata schema and fields
        schemafile =  '%s/mapfiles/b2find_schema.json' % (os.getcwd())
        with open(schemafile, 'r') as f:
            self.b2findfields=json.loads(f.read(), object_pairs_hook=OrderedDict)

    class cv_disciplines(object):
        """ cv_disciplines represents the closed vocabulary of B2FIND disciplines
        Copyright (C) 2014 Heinrich Widmann.
        """
        def __init__(self):
            self.discipl_list = self.get_list()

        @staticmethod
        def get_list():
            import csv,os
            discipl_file =  '%s/mapfiles/b2find_disciplines.tab' % (os.getcwd())
            disctab = []
            with open(discipl_file, 'r') as f:
                tsvfile = csv.reader(f, delimiter='\t') ## csv object with deliminator tab
                for line in tsvfile: ## iterate through lines in file
                   disctab.append(line)
                   
            return disctab

    def date2UTC(self,old_date):
        """
        changes date to UTC format
        """
        if type(old_date) is list :
            old_date=old_date[0]
        # UTC format =  YYYY-MM-DDThh:mm:ssZ
        try:
            new_date=''
            utc = re.compile(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z')

            utc_day1 = re.compile(r'\d{4}-\d{2}-\d{2}') # day (YYYY-MM-DD)
            utc_day = re.compile(r'\d{8}') # day (YYYYMMDD)
            utc_year = re.compile(r'\d{4}') # year (4-digit number)
            if utc.search(old_date):
                new_date = utc.search(old_date).group()
                return new_date
            elif utc_day1.search(old_date):
                day = utc_day1.search(old_date).group()
                new_date = day + 'T11:59:59Z'
                return new_date
            elif utc_day.search(old_date):
                rep=re.findall(utc_day, old_date)[0]
                new_date = rep[0:4]+'-'+rep[4:6]+'-'+rep[6:8] + 'T11:59:59Z'
                return new_date
            elif utc_year.search(old_date):
                year = utc_year.search(old_date).group()
                new_date = year + '-07-01T11:59:59Z'
                return new_date
            else:
                return '' # if converting cannot be done, make date empty
        except Exception, e:
           print('[ERROR] : %s - in date2UTC replace old date %s by new date %s' % (e,old_date,new_date))
           return ''
        else:
           return new_date

    def map_identifiers(self, invalue):
        """
        Convert identifiers to data access links, i.e. to 'Source' (ds['url']) or 'PID','DOI' etc. pp
 
        Copyright (C) 2015 by Heinrich Widmann.
        Licensed under AGPLv3.
        """
        try:
            ## idarr=invalue.split(";")
            iddict=dict()
            favurl=invalue[0]  ### idarr[0]
  
            for id in invalue :
                if id.startswith('http://data.theeuropeanlibrary'):
                    iddict['url']=id
                elif id.startswith('ivo:'):
                    iddict['IVO']='http://registry.euro-vo.org/result.jsp?searchMethod=GetResource&identifier='+id
                    favurl=iddict['IVO']
                elif id.startswith('10.'): ##HEW-??? or id.startswith('10.5286') or id.startswith('10.1007') :
                    iddict['DOI'] = self.concat('http://dx.doi.org/',id)
                    favurl=iddict['DOI']
                elif 'dx.doi.org/' in id:
                    iddict['DOI'] = id
                    favurl=iddict['DOI']
                elif 'doi:' in id: ## and 'DOI' not in iddict :
                    iddict['DOI'] = 'http://dx.doi.org/doi:'+re.compile(".*doi:(.*)\s?.*").match(id).groups()[0].strip(']')
                    favurl=iddict['DOI']
                elif 'hdl.handle.net' in id:
                    iddict['PID'] = id
                    favurl=iddict['PID']
                elif 'hdl:' in id:
                    iddict['PID'] = id.replace('hdl:','http://hdl.handle.net/')
                    favurl=iddict['PID']
                ##  elif 'url' not in iddict: ##HEW!!?? bad performance --> and self.check_url(id) :
                    ##     iddict['url']=id

            if 'url' not in iddict :
                iddict['url']=favurl
        except Exception, e:
            print('[ERROR] : %s - in map_identifiers %s can not converted !' % (e,invalue))
            return None
        else:
            return iddict

    def map_lang(self, invalue):
        """
        Convert languages and language codes into ISO names
 
        Copyright (C) 2014 Mikael Karlsson.
        Adapted for B2FIND 2014 Heinrich Widmann
        Licensed under AGPLv3.
        """

        def mlang(language):
            if '_' in language:
                language = language.split('_')[0]
            if ':' in language:
                language = language.split(':')[1]
            if len(language) == 2:
                try: return iso639.languages.get(alpha2=language.lower())
                except KeyError: pass
            elif len(language) == 3:
                try: return iso639.languages.get(alpha3=language.lower())
                except KeyError: pass
                except AttributeError: pass
                try: return iso639.languages.get(terminology=language.lower())
                except KeyError: pass
                try: return iso639.languages.get(bibliographic=language.lower())
                except KeyError: pass
            else:
                try: return iso639.languages.get(name=language.title())
                except KeyError: pass
                for l in re.split('[,.;: ]+', language):
                    try: return iso639.languages.get(name=l.title())
                    except KeyError: pass

        newvalue=list()
        for lang in invalue:
            mcountry = mlang(lang)
            if mcountry:
                newvalue.append(mcountry.name)

        return newvalue

    def map_temporal(self,invalue):
        """
        Map date-times to B2FIND start and end time
 
        Copyright (C) 2015 Heinrich Widmann
        Licensed under AGPLv3.
        """
        desc=''
        try:
          if type(invalue) is list:
              invalue=invalue[0]
          if type(invalue) is dict :
            if '@type' in invalue :
              if invalue['@type'] == 'single':
                 if "date" in invalue :       
                   desc+=' %s : %s' % (invalue["@type"],invalue["date"])
                   return (desc,self.date2UTC(invalue["date"]),self.date2UTC(invalue["date"]))
                 else :
                   desc+='%s' % invalue["@type"]
                   return (desc,None,None)
              elif invalue['@type'] == 'verbatim':
                  if 'period' in invalue :
                      desc+=' %s : %s' % (invalue["type"],invalue["period"])
                  else:
                      desc+='%s' % invalue["type"]
                  return (desc,None,None)
              elif invalue['@type'] == 'range':
                  if 'start' in invalue and 'end' in invalue :
                      desc+=' %s : ( %s - %s )' % (invalue['@type'],invalue["start"],invalue["end"])
                      return (desc,self.date2UTC(invalue["start"]),self.date2UTC(invalue["end"]))
                  else:
                      desc+='%s' % invalue["@type"]
                      return (desc,None,None)
              elif 'start' in invalue and 'end' in invalue :
                  desc+=' %s : ( %s - %s )' % ('range',invalue["start"],invalue["end"])
                  return (desc,self.date2UTC(invalue["start"]),self.date2UTC(invalue["end"]))
              else:
                  return (desc,None,None)
          else:
            outlist=list()
            invlist=invalue.split(';')
            if len(invlist) == 1 :
                try:
                    desc+=' point in time : %s' % self.date2UTC(invlist[0]) 
                    return (desc,self.date2UTC(invlist[0]),self.date2UTC(invlist[0]))
                except ValueError:
                    return (desc,None,None)
##                else:
##                    desc+=': ( %s - %s ) ' % (self.date2UTC(invlist[0]),self.date2UTC(invlist[0])) 
##                    return (desc,self.date2UTC(invlist[0]),self.date2UTC(invlist[0]))
            elif len(invlist) == 2 :
                try:
                    desc+=' period : ( %s - %s ) ' % (self.date2UTC(invlist[0]),self.date2UTC(invlist[1])) 
                    return (desc,self.date2UTC(invlist[0]),self.date2UTC(invlist[1]))
                except ValueError:
                    return (desc,None,None)
            else:
                return (desc,None,None)
        except Exception, e:
           log.debug('[ERROR] : %s - in map_temporal %s can not converted !' % (e,invalue))
           return (None,None,None)
        else:
            return (desc,None,None)

    def map_discipl(self,invalue,disctab):
        """
        Convert disciplines along B2FIND disciplinary list
 
        Copyright (C) 2014 Heinrich Widmann
        Licensed under AGPLv3.
        """
        
        retval=list()
        if type(invalue) is not list :
            inlist=re.split(r'[;&\s]\s*',invalue)
            inlist.append(invalue)
        else:
            seplist=[re.split(r"[;&]",i) for i in invalue]
            swlist=[re.findall(r"[\w']+",i) for i in invalue]
            inlist=swlist+seplist
            inlist=[item for sublist in inlist for item in sublist]
        for indisc in inlist :
           ##indisc=indisc.encode('ascii','ignore').capitalize()
           indisc=indisc.encode('utf8').replace('\n',' ').replace('\r',' ').strip().title()
           maxr=0.0
           maxdisc=''
           for line in disctab :
             try:
               disc=line[2].strip()
               r=lvs.ratio(indisc,disc)
             except Exception, e:
                 print('[ERROR] %s in map_discipl : %s can not compared to %s !' % (e,indisc,disc))
                 continue
             if r > maxr  :
                 maxdisc=disc
                 maxr=r
                 ##HEW-T                   print '--- %s \n|%s|%s| %f | %f' % (line,indisc,disc,r,maxr)
           if maxr == 1 and indisc == maxdisc :
               if verbose > 1 : print('  | Perfect match of %s : nothing to do' % indisc)
               retval.append(indisc.strip())
           elif maxr > 0.90 :
               if verbose > 1 : print('   | Similarity ratio %f is > 0.90 : replace value >>%s<< with best match --> %s' % (maxr,indisc,maxdisc))
               ##return maxdisc
               ##print(indisc.strip())
           else:
               if verbose > 1 : print('   | Similarity ratio %f is < 0.90 compare value >>%s<< and discipline >>%s<<' % (maxr,indisc,maxdisc))
               continue

        if len(retval) > 0:
            retval=list(OrderedDict.fromkeys(retval)) ## this elemenates real duplicates
            return ';'.join(retval)
        else:
            return 'Not stated' 
   
    def cut(self,invalue,pattern,nfield=None):
        """
        Invalue is expected as list. Loop over invalue and for each elem : 
           - If pattern is None truncate characters specified by nfield (e.g. ':4' first 4 char, '-2:' last 2 char, ...)
        else if pattern is in invalue, split according to pattern and return field nfield (if 0 return the first found pattern),
        else return invalue.

        Copyright (C) 2015 Heinrich Widmann.
        Licensed under AGPLv3.
        """

        outvalue=list()
        if not isinstance(invalue,list): invalue = invalue.split()
        for elem in invalue:
                if pattern is None :
                    if nfield :
                        outvalue.append(elem[nfield])
                    else:
                        outvalue.append(elem)
                else:
                    rep=re.findall(pattern, elem)
                    if len(rep) > 0 :
                        outvalue.append(rep[nfield])
                    else:
                        outvalue.append(elem)
                        
        ##else:
        ##    log.error('[ERROR] : cut expects as invalue (%s) a list' % invalue)
            ## return None

        return outvalue



    def list2dictlist(self,invalue,valuearrsep):
        """
        transfer list of strings/dicts to list of dict's { "name" : "substr1" } and
          - eliminate duplicates, numbers and 1-character- strings, ...      
        """

        dictlist=[]
        valarr=[]
        bad_chars = '(){}<>:'
        if isinstance(invalue,dict):
            invalue=invalue.values()
        elif not isinstance(invalue,list):
            invalue=invalue.split(';')
            invalue=list(OrderedDict.fromkeys(invalue)) ## this eliminates real duplicates
        for lentry in invalue :
            try:
                if type(lentry) is dict :
                    if "value" in lentry:
                        valarr.append(lentry["value"])
                    else:
                        valarr=lentry.values()
                else:
                    ##valarr=filter(None, re.split(r"([,\!?:;])+",lentry)) ## ['name']))
                    valarr=re.findall('\[[^\]]*\]|\([^\)]*\)|\"[^\"]*\"|\S+',lentry)
                for entry in valarr:
                    entry="". join(c for c in entry if c not in bad_chars)
                    if isinstance(entry,int) or len(entry) < 2 : continue
                    entry=entry.encode('utf-8').strip()
                    dictlist.append({ "name": entry.replace('/','-') })
            except AttributeError, err :
                log.error('[ERROR] %s in list2dictlist of lentry %s , entry %s' % (err,lentry,entry))
                continue
            except Exception, e:
                log.error('[ERROR] %s in list2dictlist of lentry %s, entry %s ' % (e,lentry,entry))
                continue
        return dictlist[:12]

    def uniq(self,input,joinsep=None):
        uniqset = set(input)

        ##if joinsep :
        ##    return joinsep.join(list(set))
        ##else :
        return list(uniqset)

    def evalxpath(self, obj, expr, ns):
        # evaluates and parses XML etree object for xpath expr and returns the found values
        flist=re.split(r'[\(\),]',expr.strip()) ### r'[(]',expr.strip())
        retlist=list()
        for func in flist:
            func=func.strip()
            if func.startswith('//'): 
                fxpath= '.'+re.sub(r'/text()','',func)
                try:
                    for elem in obj.findall(fxpath,ns):
                        if elem.text :
                            retlist.append(elem.text)
                except Exception as e:
                    print 'ERROR %s : during xpath extraction of %s' % (e,fxpath)
                    return []
            elif func == '/':
                try:
                    for elem in obj.findall('.//',ns):
                        retlist.append(elem.text)
                except Exception as e:
                    print 'ERROR %s : during xpath extraction of %s' % (e,'./')
                    return []

        return retlist

    def xpathmdmapper(self,xmldata,xrules,namespaces):
        # returns list or string, selected from xmldata by xpath rules (and namespaces)
        if verbose > 1 : print '   | %-10s | %-10s | %-10s |\n   -------------------------------' % ('Field','XPATH','Value')

        jsondata=dict()

        for line in xrules: # loop over xpath rules in mapfile
          try:
            m = re.match(r'(\s+)<field name="(.*?)">', line)
            if m: # next field found
                field=m.group(2)
            else: # not a <field> line
                retval=''
                strexpr = re.compile('(\s+)(<string>)(.*?)(</string>)').search(line)
                xpathexpr = re.compile('(\s+)(<xpath>)(.*?)(</xpath>)').search(line)
                if strexpr:
                    retval=strexpr.group(3)
                    xpath="'"+retval+"'"
                elif xpathexpr:
                    xpath=xpathexpr.group(3)
                    retval=self.evalxpath(xmldata, xpathexpr.group(3), namespaces)
                else:
                    continue

                if retval and len(retval) > 0 :
                    jsondata[field]=retval ### .extend(retval)
                    if verbose > 1 : print '   | %-8s | %-10s | %-20s |' % (field,xpath,retval[:20])
                else:
                    if verbose > 1 : print '   | %-8s | %-10s | %-20s |' % (field,xpath,retval[:20])

          except Exception as e:
              print '    | [ERROR] : %s in xpathmdmapper \n\tfield\t%s\n\txpath\t%s\n\tretvalue\t%s' % (e,field,line,retval)
              continue

        return jsondata

    def map(self,community, verb, mdprefix, subset):

        stats = {
            "count"    : 0,    # number of all provided datasets per subset
            "scount"     : 0,    # number of all successful mapped datasets
            "ecount"    : 0,    # number of all failed datasets
            "dcount"    : 0,    # number of all deleted datasets
            "timestart" : time.time(),  # start time
        }

        if not subset : subset='SET'
        # check data directory and create subdir for json files :
        subsetdir = '%s/%s-%s/%s' % (self.base_outdir,community,mdprefix,subset)
        if not os.path.isdir(subsetdir+'/xml') :
            print ' [ERROR] Can not access input directory %s/xml' % subsetdir
            sys.exit(-1)
        if (not os.path.isdir(subsetdir+'/json')):
            os.makedirs(subsetdir+'/json')

        # set mapfile
        mapfile='%s/mapfiles/%s-%s.xml' % (os.getcwd(),community,mdprefix)
        if not os.path.isfile(mapfile):
            print '[ERROR] Can not access mapfile %s' % mapfile
            return stats
        print ' |- Mapfile\tmapfiles/%s-%s.xml' % (community,mdprefix)
        print ' |- Output path\t%s/json' % (subsetdir)

        # get mapping rules and check namespaces
        mf = codecs.open(mapfile, "r", "utf-8")
        maprules = filter(lambda x:len(x) != 0,mf.readlines()) # removes empty lines
        namespaces=dict()
        for line in maprules:
            ns = re.match(r'(\s+)(<namespace ns=")(\w+)"(\s+)uri="(.*)"/>', line)
            if ns:
                namespaces[ns.group(3)]=ns.group(5)
                continue
        print ' |- Namespaces\t%s' % json.dumps(namespaces,sort_keys=True, indent=4)

        disctab = self.cv_disciplines() # instance of B2FIND discipline table


        # loop over all XML files (harvested records) in input path  
        files = filter(lambda x: x.endswith('.xml'), os.listdir(subsetdir+'/xml'))
        stats['count'] = len(files)
        fcount = 0        


        print ' |-Output path\t %s/json' % subsetdir 
        print '    |   | %-4s | %-45s |\n    ---------------------------------------------------------------' % ('#','File Identifier')

        for filename in files:
            fcount+=1
            jsondata = dict()
            infile='%s/xml/%s' % (subsetdir,filename)
            print '    | m | %-4d | %-45s |' % (fcount,os.path.basename(filename))

            if ( os.path.getsize(infile) > 0 ):                
                with open(infile, 'r') as f: ## load and parse raw xml rsp. json
                    try:
                        xmldata= etree.parse(infile)
                    except Exception as e:
                        print '    | [ERROR] %s : Cannot load or parse XML file %s' % (e,infile)
                        stats['ecount'] += 1
                        continue
            else:
                print 'File %s seems to be empty' % filename
                stats['ecount'] += 1
                continue

            ## XPATH converter
            try:
                # Run Python XPATH converter
                if verbose > 1 : print ' | Start XPATH selection'
                jsondata=self.xpathmdmapper(xmldata,maprules,namespaces)
            except Exception as e:
                print '    | [ERROR] %s : during XPATH processing' % e
                stats['ecount'] += 1
                continue
            ## map onto B2FIND schema
            if verbose > 1 : print '  | Map onto B2FIND schema '
            if verbose > 1 : print ('   | %-10s :  %-20s\n   ----------------------------------' % ('B2F facet','Value'))
            for facetdict in self.b2findfields.values() :
                facet=facetdict["ckanName"]
                if facet in jsondata:
                    if verbose > 1 : print ('   | %-10s : %-20s' % (facet,jsondata[facet]))
                    try:
                        facetmeth='map_'+facet
                        if facet == 'author':
                            jsondata[facet] = self.uniq(self.cut(jsondata[facet],'\(\d\d\d\d\)',1))
                            ## jsondata[facet] = getattr(self,facetmeth) 
                        elif facet == 'tags':
                            jsondata[facet] = self.list2dictlist(jsondata[facet]," ")
                        elif facet == 'url':
                            iddict = self.map_identifiers(jsondata[facet])

                            if 'DOI' in iddict :
                                if not 'DOI' in jsondata :
                                    jsondata['DOI']=iddict['DOI']
                            if 'PID' in iddict :
                                if not ('DOI' in jsondata and jsondata['DOI']==iddict['PID']):
                                    jsondata['PID']=iddict['PID']
                            if 'url' in iddict:
                                jsondata['url']=iddict['url']
                            else:
                                jsondata['url']=''

                        elif facet == 'Checksum':
                            jsondata[facet] = self.map_checksum(jsondata[facet])
                        elif facet == 'Discipline':
                            jsondata[facet] = self.map_discipl(jsondata[facet],disctab.discipl_list)
                        elif facet == 'Publisher':
                            blist = self.cut(jsondata[facet],'=',2)
                            jsondata[facet] = self.uniq(blist)
                        elif facet == 'Contact':
                            if all(x is None for x in jsondata[facet]):
                                jsondata[facet] = ['Not stated']
                            else:
                                blist = self.cut(jsondata[facet],'=',2)
                                jsondata[facet] = self.uniq(blist)
                        elif facet == 'SpatialCoverage':
                            jsondata['SpatialCoverage'],slat,wlon,nlat,elon = self.map_spatial(jsondata[facet],geotab.geonames_list)
                            if wlon and slat and elon and nlat :
                                jsondata['spatial']="{\"type\":\"Polygon\",\"coordinates\":[[[%s,%s],[%s,%s],[%s,%s],[%s,%s],[%s,%s]]]}" % (wlon,slat,wlon,nlat,elon,nlat,elon,slat,wlon,slat)
                        elif facet == 'TemporalCoverage':
                            jsondata['TemporalCoverage'],tempdesc,stime,etime=self.map_temporal(jsondata[facet])
                        elif facet == 'Language': 
                            jsondata[facet] = self.map_lang(jsondata[facet])
                        elif facet == 'Format': 
                            jsondata[facet] = self.uniq(jsondata[facet])
                        elif facet == 'PublicationYear':
                            publdate=self.date2UTC(jsondata[facet])
                            if publdate:
                                jsondata[facet] = self.cut([publdate],'\d\d\d\d',0)
                        elif facet == 'fulltext':
                            encoding='utf-8'
                            jsondata[facet] = ' '.join([x.strip() for x in filter(None,jsondata[facet])]).encode(encoding)[:32000]
                    except Exception as err:
                        print ('%s during mapping of field\t%s' % (err,facet))
                        print ('\t\tvalue%s' % (jsondata[facet]))
                        continue
                
                else: # B2FIND facet not in jsondata
                    if verbose > 1 : print ('   | %-10s : %-20s' % (facet,'N/A'))

                ## decode and convert to JSON format
                try:
                    ##HEW-T print ('decode json data')
                    data = json.dumps(jsondata,sort_keys = True, indent = 4).decode('utf-8') ## needed, else : Cannot write json file ... : must be unicode, not str
                except Exception as err:
                    print('%s : Cannot decode jsondata %s' % (err,jsondata))


                ## write to JSON file
                jsonfilename=os.path.splitext(filename)[0]+'.json'
                with open(subsetdir+'/json/'+jsonfilename, 'w') as json_file:
                    try:
                        ##HEW-T print ('Save json file')
                        json_file.write(data)
                    except (TypeError,Exception) as err:
                        print (' %s : Cannot write data to json file %s ' % (err,jsonfilename))
                        stats['ecount'] += 1
                        continue

        return stats


class CKAN_CLIENT(object):

    """
    ### CKAN_CLIENT - class
    # Provides methods to call a CKAN API request via urllib2
    # create CKAN object                       
    CKAN = CKAN_CLIENT(iphost,auth)

    # call action api example:
    CKAN.action('package_create',{"name":"testdata", "title":"empty test object"})
    """

    def __init__ (self, ip_host, api_key):
        self.ip_host = ip_host
        self.api_key = api_key
	
    def validate_actionname(self,action):
        return True
	
    def action(self, action, data={}):
        ## action (action, jsondata) - method
	    # Call the api action <action> with the <jsondata> on the CKAN instance which was defined by iphost (parameter of CKAN_CLIENT).
	    #
	    
	    if (not self.validate_actionname(action)):
		    print('Action name '+ str(action) +' is not defined in CKAN_CLIENT!')
	    else:
		    return self.__action_api(action, data)
		
    def __action_api (self, action, data_dict):
        # Make the HTTP request for data set generation.
        response=''
        rvalue = 0
        ##api_url = "http://{host}/api/rest".format(host=self.ip_host)
        ##action_url = "{apiurl}/dataset".format(apiurl=api_url)	# default for 'package_create'

        
        action_url = 'http://{host}/api/3/action/{action}'.format(host=self.ip_host,action=action)

        if verbose > 1 : print(' CKAN request:\n |- Action\t%s\n |- RequestURL\t%s\n |- Data_dict\t%s' % (action,action_url,data_dict))	

        # make json data in conformity with URL standards
        encoding='utf-8'
        ##encoding='ISO-8859-15'
        try:
            data_string = quote(json.dumps(data_dict))##.encode("utf-8") ## HEW-D 160810 , encoding="latin-1" ))##HEW-D .decode(encoding)
        except Exception as err :
            print('%s while building url data' % err)

        try:
            request = Request(action_url,data_string)
            if verbose > 1 : print('request %s' % request)            
            if (self.api_key): request.add_header('Authorization', self.api_key)
            ##print('api_key %s....' % self.api_key[:10])
            response = urlopen(request)                
            if verbose > 1 : print('response %s' % response)            
        except HTTPError as e:
            if verbose > 1 : print('%s : The server %s couldn\'t fulfill the action %s.' % (e,self.ip_host,action))
            if ( e.code == 403 ):
                print('Access forbidden, maybe the API key is not valid?')
                exit(e.code)
            elif ( e.code == 409 and action == 'package_create'):
                if verbose > 2 : print('\tMaybe the dataset already exists => try to update the package')
                self.action('package_update',data_dict)
            elif ( e.code == 409):
                print('\tMaybe you have a parameter error?')
                return {"success" : False}
            elif ( e.code == 500):
                print('\tInternal server error')
                return {"success" : False}
        except URLError as e:
            print('\tURLError %s : %s' % (e,e.reason))
            return {"success" : False}
        except Exception as e:
            print('\t%s' % e)
            return {"success" : False}
        else :
            out = json.loads(response.read())
            if verbose > 2 : print('out %s' % out)
            assert response.code >= 200
            return out

class UPLOADER(object):

    """
    ### UPLOADER - class
    # Uploads JSON files to CKAN portal and provides more methods for checking a dataset
    # create UPLOADER object:
    UP = UPLOADER(CKAN)
    """
    
    def __init__(self, CKAN, base_outdir):
        self.base_outdir = base_outdir
        self.CKAN = CKAN
        self.package_list = dict()

        # Read in B2FIND metadata schema and fields
        schemafile =  '%s/mapfiles/b2find_schema.json' % (os.getcwd())
        with open(schemafile, 'r') as f:
            self.b2findfields=json.loads(f.read())

        self.ckandeffields = ["author","title","notes","tags","url","version"]
        self.b2fckandeffields = ["Creator","Title","Description","Tags","Source","Checksum"]

    def json2ckan(self, jsondata):
        ## json2ckan(UPLOADER object, json data) - method
        ##  converts flat JSON structure to CKAN JSON record with extra fields
        if verbose > 1 : print(' Default fields:')
        for key in self.ckandeffields :
            if key not in jsondata or jsondata[key]=='':
                if verbose > 0 : print('CKAN default key %s does not exist' % key)
            else:
                if key in  ["author"] :
                    jsondata[key]=';'.join(list(jsondata[key]))
                elif key in ["title","notes"] :
                    jsondata[key]='\n'.join([x for x in jsondata[key] if x is not None])
                if verbose > 1 : print(' | %-15s | %-25s' % (key,jsondata[key]))
                        
        jsondata['extras']=list()
        extrafields=sorted(set(self.b2findfields.keys()) - set(self.b2fckandeffields))
        if verbose > 1 : print(' CKAN extra fields')
        for key in extrafields :
            if key in jsondata :
                if key in ['Contact','Format','Language','Publisher','PublicationYear','Checksum','Rights']:
                    value=';'.join(jsondata[key])
                elif key in ['oai_identifier']:
                    if isinstance(jsondata[key],list) or isinstance(jsondata[key],set) : 
                        value=jsondata[key][-1]      
                else:
                    value=jsondata[key]
                jsondata['extras'].append({
                     "key" : key,
                     "value" : value
                })
                del jsondata[key]
                if verbose > 1 : print(' | %-15s | %-25s' % (key,value))
            else:
                if verbose > 1 : print(' | %-15s | %-25s' % (key,'-- No data available'))

        return jsondata

    def check(self, jsondata):
        ## check(UPLOADER object, json data) - method
        # Checks the jsondata and returns the correct ones
        #
        # Parameters:
        # -----------
        # 1. (dict)    jsondata - json dictionary with metadata fields with B2FIND standard
        #
        # Return Values:
        # --------------
        # 1. (dict)   
        # Raise errors:
        # -------------
        #               0 - critical error occured
        #               1 - non-critical error occured
        #               2 - no error occured    
    
        errmsg = ''
        
        ## check mandatory fields ...
        mandFields=['title','oai_identifier']
        for field in mandFields :
            if field not in jsondata: ##  or jsondata[field] == ''):
                print("The mandatory field '%s' is missing" % field)
                return None

        identFields=['DOI','PID','url']
        identFlag=False
        for field in identFields :
            if field in jsondata:
                identFlag=True
        if identFlag == False:
            print("At least one identifier from %s is mandatory" % identFields)
            return None
            
        if 'PublicationYear' in jsondata :
            try:
                datetime.datetime.strptime(jsondata['PublicationYear'][0], '%Y')
            except (ValueError,TypeError) as e:
                print("%s : Facet %s must be in format YYYY, given values : %s" % (e,'PublicationYear',jsondata['PublicationYear']))
                ##HEW-D raise Exception("Error %s : Key %s value %s has incorrect data format, should be YYYY" % (e,'PublicationYear',jsondata['PublicationYear']))
                # delete this field from the jsondata:
                del jsondata['PublicationYear']
                
        # check Date-Times for consistency with UTC format
        dt_keys=['PublicationTimestamp', 'TemporalCoverage:BeginDate', 'TemporalCoverage:EndDate']
        for key in dt_keys:
            if key in jsondata :
                try:
                    datetime.datetime.strptime(jsondata[key], '%Y-%m-%d'+'T'+'%H:%M:%S'+'Z')
                except ValueError:
                    self.logger.error("Value %s of key %s has incorrect data format, should be YYYY-MM-DDThh:mm:ssZ" % (jsondata[key],key))
                    del jsondata[key] # delete this field from the jsondata
                except TypeError:
                    self.logger.error("Value %s of key %s has incorrect type, must be string YYYY-MM-DDThh:mm:ssZ" % (jsondata[key],key))
                    del jsondata[key] # delete this field from the jsondata

        return jsondata

    def upload(self, community, iphost, mdprefix, subset):
        ## upload (UPLOADER object, community, mdprefix, subset) - method

        CKAN = self.CKAN

        mdschemas={
            "ddi" : "ddi:codebook:2_5 http://www.ddialliance.org/Specification/DDI-Codebook/2.5/XMLSchema/codebook.xsd",
            "oai_ddi" : "http://www.icpsr.umich.edu/DDI/Version1-2-2.xsd",
            "marcxml" : "http://www.loc.gov/MARC21/slim http://www.loc.gov/standards",
            "iso" : "http://www.isotc211.org/2005/gmd/metadataEntity.xsd",        
            "iso19139" : "http://www.isotc211.org/2005/gmd/gmd.xsd",        
            "oai_dc" : "http://www.openarchives.org/OAI/2.0/oai_dc.xsd",
            "oai_qdc" : "http://pandata.org/pmh/oai_qdc.xsd",
            "cmdi" : "http://catalog.clarin.eu/ds/ComponentRegistry/rest/registry/profiles/clarin.eu:cr1:p_1369752611610/xsd",
            "json" : "http://json-schema.org/latest/json-schema-core.html",
            "fgdc" : "No specification for fgdc available",
            "hdcp2" : "No specification for hdcp2 available"
        }

        stats = {
            "count"    : 0,    # number of all provided datasets per subset
            "scount"     : 0,    # number of all successful uploaded datasets
            "ecount"    : 0,    # number of all failed datasets
            "dcount"    : 0,    # number of all deleted datasets
            "timestart" : time.time(),  # start time
        }

        if not subset : subset='SET'
        # check data directory and create subdir for json files :
        subsetdir = '%s/%s-%s/%s' % (self.base_outdir,community,mdprefix,subset)
        if not os.path.isdir(subsetdir+'/json') :
            print ' [ERROR] Can not access input directory %s/json' % subsetdir
            sys.exit(-1)

        try:
            ckangroup=CKAN.action('group_list')
            if community not in ckangroup['result'] :
                print('Can not found community %s' % community)
                sys.exit(-1)
        except Exception as err:
            print("[ERROR %s] : Can not list CKAN groups" % err)
        
        files = [x for x in os.listdir(subsetdir+'/json') if x.endswith('.json')]
        stats['tcount'] = len(files)

        fcount = 0
        for filename in files:
            fcount+=1
            jsondata = dict()
            infile = subsetdir+'/json/'+filename
            if ( os.path.getsize(infile) > 0 ):
                with open(infile, 'r') as f:
                    try:
                        jsondata=json.loads(f.read(),encoding = 'utf-8')
                    except:
                        print('    | [ERROR] Cannot load the json file %s' % path+'/json/'+filename)
                        stats['ecount'] += 1
                        continue
            else:
                stats['ecount'] += 1
                continue

            # get dataset id (CKAN name) from filename (a uuid generated identifier):
            ds_id = os.path.splitext(filename)[0]
            
            print('    | u | %-4d | %-40s |' % (fcount,ds_id))

            ### CHECK JSON DATA for upload
            jsondata=self.check(jsondata)
            if jsondata == None :
                print('File %s failed check and will not been uploaded' % filename)
                continue

##            jsondata['group']=community

            jsondata=self.json2ckan(jsondata)

            ckands='http://'+iphost+'/dataset/'+ds_id
        
            # add some general CKAN specific fields to dictionary:
            jsondata["name"] = ds_id
            jsondata["state"]='active'
            jsondata["groups"]=[{ "name" : community }]
            jsondata["owner_org"]="eudat"

            ## write to JSON file
            jsonfilename=os.path.splitext(filename)[0]+'.json'

            if (not os.path.isdir(subsetdir+'/ckan')):
                os.makedirs(subsetdir+'/ckan')

            with io.open(subsetdir+'/ckan/'+filename, 'w') as json_file:
                try:
                    if verbose > 2 : print('   | [INFO] decode json data')
                    data = json.dumps(jsondata,sort_keys = True, indent = 4).decode('utf8')
                except Exception as e:
                    print('    | [ERROR] %s : Cannot decode jsondata %s' % (e,jsondata))
                try:
                    if verbose > 2 : print('   | [INFO] save json file')
                    json_file.write(data)
                except TypeError, err :
                    print('    | [ERROR] Cannot write json file %s : %s' % (outpath+'/'+filename,err))
                except Exception as e:
                    print('    | [ERROR] %s : Cannot write json file %s' % (e,outpath+'/'+filename))

        
            if verbose > 2 : print('\t - Try to create dataset %s' % ds_id)
            results = self.CKAN.action('package_create',jsondata)
            if (results and results['success']):
                rvalue = 1
            else:
                if verbose > 2 : print('\t - Creation failed. Try to update instead.')
                results = self.CKAN.action('package_update',jsondata)
                if (results and results['success']):
                    rvalue = 2
                else:
                    print('\t - Update failed.')
                    rvalue = 0
        
        return rvalue

    def check_dataset(self,dsname,checksum):
        ## check_dataset (UPLOADER object, dsname, checksum) - method
        # Compare the checksum of <dsname> in CKAN portal with the given <checksum>. If they are equal 'unchanged'
        # will be returned. 
        # Otherwise returns 'new', 'changed' or 'unknown' if check failed.
        #
        # Parameters:
        # -----------
        # (string)  dsname - Name of the dataset
        #
        # Return Values:
        # --------------
        # 1. (string)  ckanstatus, can be:
        #               1. 'unknown'
        #               2. 'new'
        #               3. 'unchanged'
        #               4. 'changed'
    
        ckanstatus='unknown'
        if not (dsname in self.package_list):
            ckanstatus="new"
        else:
            if ( checksum == self.package_list[dsname]):
                ckanstatus="unchanged"
            else:
                ckanstatus="changed"
        return ckanstatus
    
    
    def check_url(self,url):
        ## check_url (UPLOADER object, url) - method
        # Checks and validates a url via urllib module
        #
        # Parameters:
        # -----------
        # (url)  url - Url to check
        #
        # Return Values:
        # --------------
        # 1. (boolean)  result
    
        try:
            resp = urlopen(url, timeout=10).getcode()###HEW-!! < 501
        except HTTPError as err:
            if (err.code == 422):
                print('%s in check_url of %s' % (err.code,url))
                return Warning
            else :
                return False
        except URLError as err: ## HEW : stupid workaraound for SSL: CERTIFICATE_VERIFY_FAILED]
            print('%s in check_url of %s' % (err,url))
            if str(err.reason).startswith('[SSL: CERTIFICATE_VERIFY_FAILED]') :
                return Warning
            else :
                return False
##        except socket.timeout as e:
#            return False    #catched
#        except IOError as err:
#            return False
        else:
            # 200 !?
            return True


def options_parser(modes):
    
    p = optparse.OptionParser(
        description = '''Description                                                              
===========                                                                           
 Management of metadata, comprising                                      
      - Harvesting of XML files from a data provider endpoint \n\t                           
      - Mapping of specially formated XML to a target JSON schema \n\t                             
      - Validation of mapped JSON records as compatible with target schema \n\t
      - Uploading of JSON records to a B2FIND instance \n\t
''',
        formatter = optparse.TitledHelpFormatter(),
        prog = 'b2find_ingest.py',
        epilog='For any further information and documentation please look at the README.md file or send an email to widmann@dkrz.de.'
    )

    group_processmodes = optparse.OptionGroup(p, "Processing modes","The script can be executed in different modes by using the option -m | --mode, and provides procedures for the whole ingestion workflow how to come from unstructured metadata to entries in the discovery portal (own CKAN or B2FIND instance).")
    group_processmodes.add_option('--mode', '-m', metavar='PROCESSINGMODE', help='\nThis specifies the processing mode. Supported modes are (h)arvesting, (m)apping, (v)alidating, and (u)ploading.')

    p.add_option('-v', '--verbose', action="count", 
                        help="increase output verbosity (e.g., -vv is more than -v)", default=False)
    p.add_option('--outdir', '-d', help="The relative root directory in which all harvested and processed files will be saved. The converting and the uploading processes work with the files from this dir. (default is 'oaidata')",default='oaidata', metavar='PATH')
    p.add_option('--community', '-c', help="community or project, for which metadata are harvested, processed, stored and uploaded. This 'label' is used through the whole metadata life cycle.", default='', metavar='STRING')
    p.add_option('--subset', help="Subset of metadata to be harvested (by default 'None') and subdirectory of harvested and processed metadata (by default 'None'",default=None, metavar='STRING')
    p.add_option('--mdprefix', help="Metadata schema of harvested meta data (default is the OAI mdprefix 'oai_dc')",default='oai_dc', metavar='STRING')
    group_single = optparse.OptionGroup(p, "Single Source Operation Mode","Use the source option if you want to ingest from only ONE source.")
    group_single.add_option('--source', '-s', help="In 'generation mode' a PATH to raw metadata given as spreadsheets or in 'harvest mode' an URL to a data provider you want to harvest metadata records from.",default=None,metavar='URL or PATH')
    group_harvest = optparse.OptionGroup(p, "Harvest Options",
        "These options will be required to harvest metadata records from a data provider (by default via OAI-PMH from the URL given by SOURCE).")
    group_harvest.add_option('--verb', help="Verbs or requests defining the mode of harvesting, can be ListRecords(default) or ListIdentifers if OAI-PMH used or e.g. 'works' if JSON-API is used",default='ListRecords', metavar='STRING')

    group_upload = optparse.OptionGroup(p, "Upload Options",
        "These options will be required to upload datasets to a CKAN repository.")
    group_upload.add_option('--iphost', '-i', help="IP adress of B2FIND portal (CKAN instance)", metavar='IP')
    group_upload.add_option('--auth', help="Authinification API key, by default taken from file $HOME/.netrc)",metavar='STRING')

    p.add_option_group(group_processmodes)
    p.add_option_group(group_single)
    p.add_option_group(group_harvest)

    return p

def main():
    # parse command line options and arguments:
    modes=['g','generate','h','harvest','m','map','v','validate','u','upload']
    options,arguments = options_parser(modes).parse_args()
    global verbose 
    verbose = options.verbose

    # set subset
    ##if options.subset == None : options.subset = 'SET'
    ## check mandatory settings
    mandParams=['community','verb','mdprefix']##,'subset'] # mandatory processing params
    for param in mandParams :
        if not getattr(options,param) :
            print " [ERROR] Processing parameter %s is required" % param
            sys.exit(-1)

    # Processing   
    if (options.mode == 'h') or (options.mode == None) :  ## HARVESTING mode:
        print '\n|- Harvesting started : %s \n |-community\t%s\n |-source\t%s\n |-verb\t\t%s\n |-MD format\t%s\n |-subset\t%s' % (time.strftime("%Y-%m-%d %H:%M:%S"),options.community,options.source,options.verb,options.mdprefix,options.subset)
        HV = HARVESTER(options.outdir)
        results = HV.harvest(options.community,options.source,options.verb,options.mdprefix,options.subset)
        print results
    if (options.mode == 'm') or (options.mode == None) :  ## MAPPING mode:
        print '\n|- Mapping started : %s \n |-community\t%s\n |-MD format\t%s\n |-subset\t%s' % (time.strftime("%Y-%m-%d %H:%M:%S"),options.community,options.mdprefix,options.subset)
        MP = MAPPER(options.outdir)
        results = MP.map(options.community,options.verb,options.mdprefix,options.subset)
        print results
    if (options.mode == 'u') or (options.mode == None) :  ## UPLOADING mode:
        print '\n|- Uploading started : %s\n |-community\t%s\n |-subset\t%s\n |-Catalog\t%s' % (time.strftime("%Y-%m-%d %H:%M:%S"),options.community,options.subset,options.iphost)
        CKAN = CKAN_CLIENT(options.iphost,options.auth)
        UP = UPLOADER(CKAN,options.outdir)
        results = UP.upload(options.community,options.iphost,options.mdprefix,options.subset)
    
if __name__ == "__main__":
    main()
