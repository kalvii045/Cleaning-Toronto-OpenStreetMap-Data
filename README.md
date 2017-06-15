# Cleaning-Toronto-OpenStreetMap-Data
This code cleans OpenStreetMap data for Toronto. The purpose is to look for inconsistencies, uniformity and accuracy in the data. 
This is the final project for Udacity's data wrangling course in the data analyst nanodegree program

The project uses Python to clean, store and analyze the data. First the data is extracted from OpenStreetMap for a particular area, 
in this case Toronto. The map data is downloaded in a XML file. Using Python, we iterate through the data and first get an idea
of what the data is like e.g. get a count of the number of node and way tags in the XML file. Then information from the tags 
are extracted, things like id, key and value. These values are stored in a database and then into a csv file. Then we search for
inconsistencies in the data, finding patterns and providing insights into the data. Read the report1.pdf and 
code_for_analysis.py files to see the full process of cleaning this dataset. 
