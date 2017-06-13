import xml.etree.cElementTree as ET
import pprint 
from collections import defaultdict
import re
import csv
import codecs
import pprint

import cerberus

import schema

## Creating a sample file 
k = 140

def get_element(osm_file, tags=('node', 'way', 'relation')):
    context = iter(ET.iterparse(osm_file, events=('start','end')))
    _, root = next(context)
    for event,elem in context:
        if event == 'end' and elem.tag in tags:
            yield elem  
            root.clear() 

with open('toronto_sample.osm', 'wb') as output:
    output.write('<?xml version="1.0" encoding="UTF-8"?>\n')
    output.write('<osm>\n  ')
    
    for i,element in enumerate(get_element('toronto_canada.osm')):
        if i % k == 0:
            output.write(ET.tostring(element, encoding='utf-8'))
    
    output.write('</osm>')

## Get all unique tags (from case study, mapparser.py)
import xml.etree.cElementTree as ET

def count_tags(filename):
        # YOUR CODE HERE
        tags = {}
        parser = ET.iterparse(filename) 
        for __, elem in parser:
            if elem.tag in tags:
                tags[elem.tag] += 1
            else:
                tags[elem.tag] = 1
                
            elem.clear()
        del parser
        return tags 
    

## Get all unique k values 
def get_unique_k_values(filename):
    osm_file = open(filename,"r")
    name_types = set()
    for __, elem in ET.iterparse(filename, events=("start",)):
        
        if elem.tag == "node" or elem.tag == "way" or elem.tag == "relation":
            for tag in elem.iter('tag'):
                try:
                    name_types.add(tag.attrib['k'])
                except KeyError:
                    continue 
                        
    print name_types

## Run get_unique_k_values('toronto_sample.osm') to see all the different k values 
count_tags("toronto_sample.osm") 

## Taking out v values to check for inconsistencies

street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)
street_types = defaultdict(int)

def audit_street_type(street_types, street_name): 
    m = street_type_re.search(street_name)
    if m:
        street_type = m.group()
        street_types[street_type] += 1
        
def print_sorted_dict(d):
    keys = d.keys()
    keys = sorted(keys, key=lambda s:s.lower())
    for k in keys:
        v = d[k]
        print "%s: %d" % (k,v)

## Use this function to check if it has the tag value we're looking for in audit1
def is_name(elem, name_type):
    return (elem.attrib['k'] == name_type)

## Find the number of and different values of k with a certain key
def audit1(name_type):
    osm_file = open("toronto_sample.osm") 
    print name_type, "\n"
    for event, elem in ET.iterparse(osm_file, events=("start",)):
        if elem.tag == "way" or elem.tag == "node" or elem.tag == "relation":
            for tag in elem.iter("tag"):
                if is_name(tag, name_type):
                    audit_street_type(street_types, tag.attrib['v'])
    
    osm_file.close() 
    print_sorted_dict(street_types)
    street_types.clear()


audit1("addr:province")

## Handle street error (code structure taken from case study, audit.py)
street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)


expected = ["Street", "Avenue", "Boulevard", "Drive", "Court", "Place", "Square", "Lane", "Road", 
            "Trail", "Parkway"]

## Convert the street types in the map to the preffered one
mapping = { "St": "Street", 
            "Rd":"Road",
            "Dr.":"Drive", 
             "W": "West", 
               "N":"North", 
               "E":"East"
            }

def audit_street_type1(street_types, street_name):
    m = street_type_re.search(street_name)
    if m:
        street_type = m.group()
        if street_type not in expected:
            street_types[street_type].add(street_name)

def is_street_name(elem):
    return (elem.attrib['k'] == "addr:street")


def audit(osmfile):
    osm_file = open(osmfile, "r")
    street_types = defaultdict(set)
    for event, elem in ET.iterparse(osm_file, events=("start",)):

        if elem.tag == "node" or elem.tag == "way":
            for tag in elem.iter("tag"):
                if is_street_name(tag):
                    audit_street_type1(street_types, tag.attrib['v'])
    osm_file.close()
    return street_types

## Updates last part of the given street accordingly
def update_name(name, mapping):

    m = street_type_re.search(name)
    better_name = name
    if m:
        better_street_type = mapping[m.group()] 
        better_name = street_type_re.sub(better_street_type,name)

    return better_name

## This cell cleans and inserts the information from the xml file to csv file (code structure taken from case 
## study and github https://github.com/davidventuri/udacity-dand/blob/master/p3/process_osm.py#L384)

NODES_PATH = "nodes.csv"
NODE_TAGS_PATH = "nodes_tags.csv"
WAYS_PATH = "ways.csv"
WAY_NODES_PATH = "ways_nodes.csv"
WAY_TAGS_PATH = "ways_tags.csv"

LOWER_COLON = re.compile(r'^([a-z]|_)+:([a-z]|_)+')
PROBLEMCHARS = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')

NODE_FIELDS = ['id', 'lat', 'lon', 'user', 'uid', 'version', 'changeset', 'timestamp']
NODE_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_FIELDS = ['id', 'user', 'uid', 'version', 'changeset', 'timestamp']
WAY_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_NODES_FIELDS = ['id', 'node_id', 'position']


def get_element(osm_file, tags=('node', 'way','relation')):
    context = ET.iterparse(osm_file, events=('start','end'))
    _,root = next(context)
    
    for event,elem in context:
        if event == "end" and elem.tag in tags:
            yield elem 
            root.clear()

class UnicodeDictWriter(csv.DictWriter, object):
    """Extend csv.DictWriter to handle Unicode input"""

    def writerow(self, row):
        super(UnicodeDictWriter, self).writerow({
            k: (v.encode('utf-8') if isinstance(v, unicode) else v) for k, v in row.iteritems()
        })

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


def load_new_tag(element,secondary,default_tag_type):
    new = {}
    new['id'] = element.attrib['id']
    if ":" not in secondary.attrib['k']:
        new['key'] = secondary.attrib['k']
        new['type'] = default_tag_type 
    
    else:
        post_colon = secondary.attrib['k'].index(":") + 1
        new['key'] = secondary.attrib['k'][post_colon:]
        new['type'] = secondary.attrib['k'][:post_colon-1]
    
    ## Hanldes street type errors
    if is_street_name(secondary):
        try:
            street_name = update_name(secondary.attrib['v'],mapping)
            new['value'] = street_name
        except KeyError:
            new['value'] = secondary.attrib['v']
    
    ## Handle province name errors
    elif new['key'] == 'province' or new['key'] == 'state':
        inconsistent_name = ['on', 'On', 'Onatrio', 'Ontario','ontario']
        province_name = secondary.attrib['v']
        if province_name is in inconsistent_name:
            province_name = 'ON'
        new['value'] = province_name
    
    ## Handle cuisine type errors
    elif new['key'] == 'cuisine':
        value1 = secondary.attrib['v']
        if value1 == 'arab':
            value1 = 'arabic'
        elif value1 == 'asian;japanese':
            value1 = 'japanese'
        elif value1 == 'vietmanese':
            value1 = 'vietnamese'
        elif value1 == 'afghani':
            value1 = 'afghan'
            
        new['value'] = value1 
    
    ## Handle country name errors Converts Canada to "CA"
    elif new['key'] == 'country':
        value1 = secondary.attrib['v']
        if value1 == 'Canada':
            value1 = "CA"
        
        new['value'] = value1
    
    else:
        new['value'] = secondary.attrib['v'] 
        
    return new 
    

def shape_element(element,node_attr_fields=NODE_FIELDS,way_attr_fields=WAY_FIELDS,problem_chars=PROBLEMCHARS,
                 default_tag_type='regular'):
    
    node_attribs = {}
    way_attribs = {}
    way_nodes = []
    tags = []
    
    if element.tag == "node":
        for attrib,value in element.attrib.iteritems():
            if attrib in node_attr_fields:
                node_attribs[attrib] = value
        
        for secondary in element.iter():
            if secondary.tag == 'tag':
                if problem_chars.match(secondary.attrib['k']) is not None:
                    continue
                else:
                    new = load_new_tag(element, secondary, default_tag_type)
                    if new is not None:
                        tags.append(new)
    
        return {'node':node_attribs, 'node_tags':tags}
    
    if element.tag=="way":
        for attrib,value in element.attrib.iteritems():
            if attrib in way_attr_fields:
                way_attribs[attrib] = value
        
        counter = 0
        for secondary in element.iter():
            if secondary.tag == "tag":
                if problem_chars.match(secondary.attrib['k']) is not None:
                    continue
                else:
                    new = load_new_tag(element, secondary, default_tag_type)
                    if new is not None:
                        tags.append(new)
            
            if secondary.tag == 'nd':
                newnd = {}
                newnd['id'] = element.attrib['id']
                newnd['node_id'] = secondary.attrib['ref']
                newnd['position'] = counter
                counter += 1
                way_nodes.append(newnd)
        
        return {'way':way_attribs, 'way_nodes':way_nodes, 'way_tags':tags} 
        
def process_map(file_in, validate):
    
    with codecs.open(NODES_PATH, "w") as nodes_file,codecs.open(NODE_TAGS_PATH, "w") as nodes_tags_file,codecs.open(WAYS_PATH, "w") as ways_file, codecs.open(WAY_NODES_PATH, "w") as way_nodes_file,codecs.open(WAY_TAGS_PATH, "w") as way_tags_file:
            
        nodes_writer = UnicodeDictWriter(nodes_file, NODE_FIELDS)
        node_tags_writer = UnicodeDictWriter(nodes_tags_file, NODE_TAGS_FIELDS)
        ways_writer = UnicodeDictWriter(ways_file, WAY_FIELDS)
        way_tags_writer = UnicodeDictWriter(way_tags_file, WAY_TAGS_FIELDS)
        way_nodes_writer = UnicodeDictWriter(way_nodes_file, WAY_NODES_FIELDS)
        
        nodes_writer.writeheader()
        node_tags_writer.writeheader()
        ways_writer.writeheader()
        way_nodes_writer.writeheader()
        way_tags_writer.writeheader()
        
        validator = cerberus.Validator()
        
        for element in get_element(file_in, tags=('node', 'way')):
            el = shape_element(element)
            if el:
                
                if element.tag == 'node':
                    nodes_writer.writerow(el['node'])
                    node_tags_writer.writerows(el['node_tags'])
                elif element.tag=='way':
                    ways_writer.writerow(el['way'])
                    way_nodes_writer.writerows(el['way_nodes'])
                    way_tags_writer.writerows(el['way_tags'])
                    
                    

process_map("toronto_sample.osm", validate=True)

import sqlite3
import csv
from pprint import pprint

sqlite_file = "mydv.db"
conn = sqlite3.connect(sqlite_file)
cur = conn.cursor() 

## Load nodes.csv into database
cur.execute('DROP TABLE IF EXISTS nodes')
conn.commit()
cur.execute(''' CREATE TABLE nodes(id INTEGER, lat FLOAT, lon FLOAT, user TEXT, uid INTEGER, version INTEGER,
            changeset INTEGER, timestamp TEXT)''')
conn.commit()
with open("nodes.csv", "rb") as fin:
    dr = csv.DictReader(fin)
    to_db = [(i['id'].decode("utf-8"), i['lat'].decode("utf-8"), i['lon'].decode("utf-8"), 
              i['user'].decode("utf-8"), i['uid'].decode("utf-8"),i['version'].decode("utf-8"),
              i['changeset'].decode("utf-8"),i['timestamp']) for i in dr]
cur.executemany("INSERT INTO nodes(id,lat,lon,user,uid,version,changeset,timestamp) VALUES(?,?,?,?,?,?,?,?);", to_db)
conn.commit() 

## Load ways_nodes.csv into database
cur.execute('DROP TABLE IF EXISTS ways_nodes')
conn.commit()
cur.execute(''' CREATE TABLE ways_nodes(id INTEGER, node_id INTEGER, position INTEGER)''')
conn.commit()
with open("ways_nodes.csv", "rb") as fin:
    dr = csv.DictReader(fin)
    to_db = [(i['id'].decode("utf-8"), i['node_id'].decode("utf-8"), i['position'].decode("utf-8")) for i in dr]
cur.executemany("INSERT INTO ways_nodes(id,node_id,position) VALUES(?,?,?);", to_db)
conn.commit() 

## Load ways_tags.csv into database
cur.execute('DROP TABLE IF EXISTS ways_tags')
conn.commit()
cur.execute('''CREATE TABLE ways_tags(id INTEGER,key TEXT,value TEXT,type TEXT)''')
conn.commit()
with open("ways_tags.csv", "rb") as fin:
    dr = csv.DictReader(fin)
    to_db = [(i['id'].decode("utf-8"), i['key'].decode("utf-8"),i['value'].decode("utf-8"),i['type'].decode("utf-8")) for i in dr]
cur.executemany("INSERT INTO ways_tags(id,key,value,type) VALUES(?,?,?,?);", to_db)
conn.commit() 

## Load ways.csv into database
cur.execute('DROP TABLE IF EXISTS ways')
conn.commit()
cur.execute(''' CREATE TABLE ways(id INTEGER, user TEXT, uid INTEGER, version INTEGER, changeset INTEGER, timestamp TEXT)''')
conn.commit()
with open("ways.csv", "rb") as fin:
    dr = csv.DictReader(fin)
    to_db = [(i['id'].decode("utf-8"),i['uid'].decode("utf-8"), i['user'].decode("utf-8"),i['version'].decode("utf-8"), i['changeset'].decode("utf-8"), i['timestamp'].decode("utf-8")) for i in dr]
cur.executemany("INSERT INTO ways(id, uid, user, version, changeset, timestamp) VALUES(?,?,?,?,?,?);", to_db)
conn.commit()

## Load nodes_tags.csv into database 
cur.execute('DROP TABLE IF EXISTS nodes_tags')
conn.commit()
cur.execute('''CREATE TABLE nodes_tags(id INTEGER,key TEXT,value TEXT,type TEXT, FOREIGN KEY (id) REFERENCES nodes(id)) ''')
conn.commit() 
with open("nodes_tags.csv",'rb') as fin:
    dr = csv.DictReader(fin)
    to_db = [(i['id'].decode("utf-8"), i['key'].decode("utf-8"), i['value'].decode("utf-8"), i['type'].decode("utf-8")) for i in dr]
cur.executemany("INSERT INTO nodes_tags(id,key,value,type) VALUES(?,?,?,?);",to_db)
conn.commit() 


import math as mt 
%pylab inline 
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt 

print "INTERESTING FACTS", "\n" 

## Number of unique users
cur.execute("SELECT COUNT(DISTINCT(uid)) FROM (SELECT uid FROM nodes UNION ALL SELECT uid FROM ways);")
all_uid = cur.fetchall()
print "Number of unique users"
print all_uid[0][0], "\n" 

cur.execute("SELECT uid, COUNT(uid) FROM (SELECT uid FROM nodes UNION ALL SELECT uid FROM ways)")


## Number of nodes
cur.execute("SELECT COUNT(*) FROM nodes;")
all_nodes = cur.fetchall()
print "Number of nodes"
print all_nodes[0][0], "\n"

## Number of ways 
cur.execute("SELECT COUNT(*) FROM ways;")
all_ways = cur.fetchall()
print "Number of ways"
print all_ways[0][0], "\n" 


## Different tourist spots
cur.execute("SELECT *, COUNT(value) FROM nodes_tags WHERE key ='tourism' GROUP BY value")
all_rows = cur.fetchall()
print "Tourist spots in Toronto"
print all_rows, "\n" 

cur.execute("SELECT COUNT(*) FROM nodes_tags WHERE key='tourism' ") 
all_tours = cur.fetchall()
print "Number of different tourist spots"
print all_tours[0][0], "\n"

## Graph different tourist spots 
tourism = {}
for tours in all_rows:
    tourism[tours[2]] = tours[4]

plt.figure(figsize=(15,5))
plt.bar(range(len(tourism)), tourism.values(), align='center')
plt.xticks(range(len(tourism)), tourism.keys())
plt.xlabel("Tourist spots")
plt.ylabel("# in city")
plt.title("Tourism") 

## Different types of cuisines
cur.execute("SELECT value, COUNT(value) FROM nodes_tags WHERE key='religion' GROUP BY value")
all_religions = cur.fetchall()
print "Religious spots in toronto"
print all_religions, "\n"

religions = {}

for religion in all_religions:
    religions[religion[0]] = religion[1]

plt.figure(figsize=(15,5))
plt.bar(range(len(religions)), religions.values(),align='center')
plt.xticks(range(len(religions)), religions.keys())
plt.title("Religious spots")