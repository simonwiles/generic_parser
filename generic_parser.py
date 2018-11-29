#!/usr/bin/python

#A generic XML to SQL query parser

#Designed to convert an XML file into a set of queries based on a configuration file.

#TO DO - Warning/error logging
#TO DO - Database mode toggle between MySQL and postgreSQL
#TO DO - More intensive testing, ensure record numbers match, spot checking full records, etc

import lxml.etree as etree
import os
import sys
import datetime
import csv
from string import Template
from optparse import OptionParser
reload(sys)
sys.setdefaultencoding('utf-8')

#Set up the dictionaries as global so we're not endlessly passing them back and forth. We write these in the ReadConfig process, then just read from there on in.

table_dict = {}
value_dict = {}
ctr_dict = {}
attrib_dict = {}
attrib_defaults = {}
file_number_dict = {}

#Set these globally, but may need to change them via options between MySQL and PostgreSQL

table_quote = '"'
value_quote = "'"

def main():
	#STEP 1 - process the options

	usage = "usage: %prog [options] arg"
	parser = OptionParser(usage)
	parser.add_option("-f", "--file", dest="filename", help="parse a single file")
	#-f optional, but must be either -f or -d, defines a single file to parse
	parser.add_option("-d", "--directory", dest="directory", help="a directory in which all XML files will be parsed.")
	#-d optional, but must be either -f or -d, defines a directory in which every xml file should be parsed. Not recursive into subdirectories
	parser.add_option("-c", "--config", dest="config_file", help="configuration file")
	#-c REQUIRED, defines the configuration file mapping from XML to DB
	parser.add_option("-t", "--template", dest="template_file", help="template file")
	#-t REQUIRED, defined the SQL template to write into
	#make this optional?
	parser.add_option("-o", "--output", dest="output", help="output file or directory")
	#-o REQUIRED, can be either a directory, or if a single-file run, a file name.
	parser.add_option("-p", "--parent", dest="parent", help="Name of the parent tag (tag containing the group of records")
	#-p optional, marks the container tag for a collection of records, would not be used for single record files
	parser.add_option("-r", "--record", dest="record", help="Name of the tag that defines a single record")
	#-r REQUIRED, the tag that defines an individual record
	parser.add_option("-n", "--namespace", dest="namespace", help="Namespace of the XML file")
	#-n optional, if the XML has a namespace, give it here. Assumes a single namespace for the entire file
	parser.add_option("-i", "--identifier", dest="identifier", help="Name of the tag whose value contains the unique identifier for the record")
	#-i REQUIRED, the tag that gives the unique identifier for the record. If this is a direct child of the record root, just give the child name, otherwise, starting at that level, give the path.
	parser.add_option("-l", "--file_number", dest="file_number_sheet", help="CSV file with the file name to file number lookup")
	#-l optional, ran out of good letters, required to use file numbers\
	parser.add_option("-m", "--database_mode", dest="database_mode", help="MySQL or Postgres, defaults to Postgres")
	#-m, database mode, a toggle between MySQL and PostgreSQL?
	parser.add_option("-s", "--single_trans", dest="single_trans", help="If true, will enable one transaction per file, cannot have transaction statements in the template, if so")
	#-s, wraps the entire file's output into a single transaction, good for speed if DB has an autocommit that you can't disable.
	parser.add_option("-z", "--recurse", dest="recurse", help="If true and a directory is set, the parser will search subdirectories for XML files to parse as well, ignored for single file parse")
	#-z, gives the option to recurse through a directory as opposed to just reading the core output.
	(options, args) = parser.parse_args()
	#Read the configuration

	#need to fail if these are blank
	template_file = options.template_file
	config_file = options.config_file
	if options.single_trans is None:
		single_trans = False
	else:
		single_trans = options.single_trans

	#process -f and -d as a pair, create a file list and prepare for output.

	if options.filename is None and options.directory is None:
		print("ERROR: No target to parse")

	if options.filename is not None and options.directory is not None:
		print("ERROR: Both target file and directory specified")

	if options.filename is not None:
		mode = "file"
		filelist = [options.filename]
		if os.path.isdir(options.output):
			output_file = None
			output_dir = options.output
		else:
			output_dir = None
			output_file = options.output

	if options.directory is not None:
		mode = "directory"
		output_file = None
		filelist = getXmlFiles(options.directory, options.recurse)	
		
		if os.path.isfile(options.output):
			print("ERROR: Directory run requires directory for output.")
		else:
			output_dir = options.output

	#DO NOT LIKE THIS, don't really want to keep passing it around either, though. Not sure how to handle this one.
	global table_quote
	if str(options.database_mode).lower()== "mysql":
		db_mode = "mysql"
		table_quote = "`"
	else:
		db_mode = "postgres"
		table_quote = '"'

	#is it possible to get the namespace from the XML directly? It'd save a configuration option
	#test with a blank namespace
	if options.namespace is not None:
		namespace = options.namespace
	else:
		namespace = ''

	#root tag can be empty, but rec and id need to be present

	root_tag = options.parent
	rec_tag = options.record
	id_tag = options.identifier

	#convert the file number sheet to a dictionary for speedy lookup

	if options.file_number_sheet is not None:
		print("File numbers found")
		#convert this into a dictionary
		with open(options.file_number_sheet, mode='r') as infile:
			reader = csv.reader(infile)
			file_number_lookup = dict((rows[0],rows[1]) for rows in reader)
		infile.close()
	else:
		file_number_lookup={}

	#STEP 2 - Convert the config file into lookup tables
	#write lookup tables for table creation, counters, value and attribute parsing
	
	root = etree.parse(open(config_file)).getroot()

	root_element = root.tag
	ReadConfig(root, "", namespace)
	#we have our lookups, so we no longer need the XML config file, get it out of memory
	del root

	#get the template from the file
	tfile = open(template_file)
	template = Template(tfile.read())
	
	#STEO 3 - Parse the file(s)
	#now that we have lookups, we start with the files themselves

	for filename in filelist:
		#set the outputtarget
		if mode == "file":
			if output_file is not None:
				outputtarget = output_file
			else:
				outputtarget = os.path.join(output_dir, os.path.split(filename)[1][:-4] + "-queries.txt")
		else:
			outputtarget = os.path.join(output_dir, os.path.split(filename)[1][:-4] + "-queries.txt")
			#if options-output is a file, just use that name
			#otherwise, get the file name, remove .xml and append -queries.txt, then join that to the output directory
		#if it's part of a directory run

		#test the input file
		try:
			with open(filename): pass
		except IOError:
			try:
				with open(filename): pass
			except IOError:
				print("Error")
				continue

		#open the output file
		#disable unique constraint checking

		output = open(outputtarget, "w")
		if db_mode == "mysql":
			output.write("SET unique_checks=0;\n")
			output.write("SET autocommit=0;\n")
		if single_trans:
			output.write("BEGIN;\n")

		#get file number
		shortfilename = os.path.split(filename)[1]
		if shortfilename in file_number_lookup:
			file_number = file_number_lookup[shortfilename]
		else:
			file_number = -1
	
		print ("Parsing file: %s") % (filename)
		print ("Start time: %s") % (datetime.datetime.now())
		events=("start", "end")
		path_note = []	
		#the root of what we're processing may not be the root of the file itself
		#we need to know what portion of the file to process
		#we assume that there is only one of these, but it need not necessarily be true, I think.

		if root_tag is None:
			#if there is no root tag, then we've only got one record and we process everything
			process= True
		else:
			#we need to split this into a list of tags by "/".
			root_path = root_tag.split("/")
			root_path = [namespace + s for s in root_path]
			process = False
		
		#The recover ability may or may not be available based on the version of lxml installed. Try to use it, but if not, go without
		try:	
			parser= etree.iterparse(filename, remove_comments=True, recover=True, events=events)
		except:
			parser= etree.iterparse(filename, remove_comments=True, events=events)

		for event, elem in parser:
			#Here we keep an eye on our path.
			#If we have a root path defined, then we build a path as we go
			#If we are opening a tag that matches the root path, then we set processing to true
			#If we close a tag that matches the root path, then we set processing to false

			#if there is no root path, then we set process to true earlier and just leave it that way
			if root_tag is not None:
				if event == "start":
					#add the new element to the current path
					path_note.append(elem.tag)
					#if the path matches the root path, then we have reached an area of interest, set processing to true
					if path_note == root_path:
						process = True		
				elif event == "end":
					#if the path equals the root path, then we are leaving an area of itnerest, set processing to false
					if path_note == root_path:
						process = False
					#remove the last element from the current path
					path_note.pop()

			#iteratively parse through the XML, focusing on the tag that starts a record
			#pass over things outside the processing area. Only process end tags.
			if event=="end" and  process == True:
				if elem.tag == "%s%s" % (namespace, rec_tag):
					
					#you've got a record, now parse it
	
					tableList = TableList()
					statementList = []
	
					#lookups are by full path, so we need to maintain knowledge of that path
					#by definition, we must by on the root path, then the record tag
	
					if root_tag is not None:
						path = "%s%s/%s" % (namespace, root_tag, rec_tag)
					else:
						path = "%s%s" % (namespace, rec_tag)
					table_path = "%s/%s" % (path, 'table')
					file_number_path = "%s/%s" %  (path, 'file_number')
				        valuepath = path + "/"
	
					#get the core table name from the lookup
					core_table_name = table_dict[table_path]
	
					#create the core table
					tableList.AddTable(core_table_name, None, path)
	
					#get the primary key
					#the head tag may be the identifier, if so, just grab it, otherwise, seek it out
					if id_tag != rec_tag:
						id_seek = "%s%s" % (namespace, id_tag)
						id_node = elem.find(id_seek)
						id_value = "'" + id_node.text + "'"
					else:				
						id_value = "'" + elem.text + "'"
	
					#set the primary key
					tableList.AddIdentifier(core_table_name, 'id', id_value)
	
					#see if this table needs a file number
					if file_number_path in file_number_dict:
						file_number_name = file_number_dict[file_number_path]
						tableList.AddCol(file_number_name.split(":",1)[0],file_number_name.split(":",1)[1], file_number)
					
					attribSeen = set()
					#process the attributes
					for attribName, attribValue in elem.attrib.items():
						attribpath = path+ "/"+attribName
						if attribpath in attrib_dict:
							tableName, colName = attrib_dict[attribpath].split(":")[:2] 
							tableList.AddCol(tableName, colName, str(attribValue))
							attribSeen.add(attribName)
					
					#process default attribute values
					for attribName, attribValueAll in attrib_defaults.get(path, {}).items():
						if attribName not in attribSeen:
							tableName, colName, attribValue = attribValueAll.split(":")[:3]
							tableList.AddCol(tableName, colName, str(attribValue))
	
					#process the value
					if valuepath in value_dict:
						if node.text is not None:
							tableList.AddCol(value_dict[valuepath].split(":",1)[0],value_dict[valuepath].split(":",1)[1], str(node.text))
	
					#process the children
					for child in elem:
						ParseNode(child, path, tableList, core_table_name, statementList)
	
					#close the primary table
					tableList.CloseTable(core_table_name, statementList)
				
					#write out the statements in reverse order to ensure key compliance
	
					data = ""
					for statement in reversed(statementList):
							data = data + (str(statement) + "\n")

					#set the values that might be used in the template
	
					template_dict={}
					template_dict['data'] = data
					template_dict['file_number'] = file_number
					template_dict['id'] = id_value
	
					#write the data out intpo the template and write to file
	
					final = template.substitute(template_dict)
					output.write(final)
					
					#clear memory

					output.flush()
					elem.clear()
					#finished individual record
			if elem.getparent() is None and event == "end":
				break
				#some versions of lxml run off the end of the file. This forces the for loop to break at the root.

		#reenable unique constraint checking and close the output file
		if db_mode == "mysql":
			output.write("SET unique_checks=1;\n")			
			output.write("SET autocommit=1;\n")
		if single_trans:
			output.write("COMMIT;\n")
		output.close()
		print("End time: %s") % (datetime.datetime.now())
			
		
def ParseNode(node, path, tableList, last_opened, statementList):
	#recursive node parser
	#given a node in a tree known not to be the record tag, prase it and its children

	#first, update the path from parent, for use in lookups
	if node.tag.find("}") >-1:
		tag = node.tag.split("}",1)[1]
	else:
		tag = node.tag
	newpath = path + "/" + tag
	#see if we need a new table, make sure children inherit the right parent
	table_path = newpath + "/" + "table"
	valuepath = newpath + "/"
	#See if this tag requires a new table

	if table_path in table_dict:
		new_table = True
		table_name = table_dict[table_path]
		tableList.AddTable(table_name, last_opened, newpath)
	else:
		new_table = False
		table_name = last_opened

	#See if this tag calls for a file number

	if newpath in file_number_dict:
		file_number_name = file_number_dict[newpath]
		tableList.AddCol(file_number_name.split(":",1)[0],file_number_name.split(":",1)[1], file_number)

	#process attributes
	attribSeen = set()
	for attribName, attribValue in node.attrib.items():
		attribpath = newpath+ "/"+attribName
		if attribpath in attrib_dict:
			tableName, colName = attrib_dict[attribpath].split(":")[:2] 
			tableList.AddCol(tableName, colName, str(attribValue))
			attribSeen.add(attribName)
	
	#process default attribute values
	for attribName, attribValueAll in attrib_defaults.get(newpath, {}).items():
		if attribName not in attribSeen:
			tableName, colName, attribValue = attribValueAll.split(":")[:3]
			tableList.AddCol(tableName, colName, str(attribValue))

	#process value
	if valuepath in value_dict:
		if node.text is not None:
			tableList.AddCol(value_dict[valuepath].split(":",1)[0],value_dict[valuepath].split(":",1)[1], str(node.text))
	
	#process children
	for child in node:
		ParseNode(child, newpath, tableList, table_name, statementList)

	#if we created a new table for this tag, now it's time to close it.	
	if new_table == True:
		tableList.CloseTable(table_name, statementList)


def ReadConfig(node, path, namespace):

	#This recursive function will go through the config file, reading each tag and attribute and create the needed lookup tables
	#All tags and attributes are recorded by full path, so name reusage shouldn't be a problem

	newpath = path+ node.tag+ "/"

	#write the value lookup for the tag
	if node.text is not None:
		if str(node.text).strip() > '':
			value_dict["%s%s" % (namespace, newpath)] = node.text

	#go through the attributes in the config file
	#specialized ones like table and ctr_id go into their own lookups, the rest go into the attribute lookup
	for attribName, attribValueAll in node.attrib.items():
		attribValue = ':'.join(attribValueAll.split(':')[:2])

		attrib_path = newpath + attribName
		if attribName == "table":
			table_dict["%s%s" % (namespace, attrib_path)]= attribValue
		elif attribName == "ctr_id":
			ctr_dict["%s%s" % (namespace, attrib_path)] = attribValue
		elif attribName == "file_number":
			file_number_dict["%s%s" % (namespace, attrib_path)] = attribValue
		else:
			attrib_dict["%s%s" % (namespace, attrib_path)] = attribValue

			# Providing a third tuple item specifies the default value for that attribute
			# If the attribute isn't found in the data, use the default value instead.
			if len(attribValueAll.split(':')) == 3:
				defaults = attrib_defaults.setdefault(newpath.strip('/'), {})
				defaults[attribName] = attribValueAll

	#Now recurse for the children of the node
	for child in node:
		ReadConfig(child, newpath, namespace)


class TableList:
	#The TableList is the memory structure that stores the data as we read it out of XML
	#This is the only way that we handle the Tables that we're creating during the main process.
	#We can never have more than one instance of a table with the same name
	#When a tag that needs a table opens, we call AddTable.
	#AddIdentifier should only be needed for the master table. Identifiers are added automatically after that.
	#AddCol is used for each value that we detect
	#When a tag that created a table closes, we call CloseTable for that table. This kicks the insert statement out to the stack and frees up that table name if needed again.

	def __init__(self):
		self.tlist = []
	def AddTable (self, tableName, parentName, tablePath):
		t = Table(tableName, parentName, self, tablePath)
		self.tlist.append(t)
	def AddCol (self, tableName, colName, colValue):
		for t in self.tlist:
			if t.name == tableName:
				t.AddCol(colName, colValue)
	def AddIdentifier (self, tableName, colName, colValue):
		for t in self.tlist:
			if t.name == tableName:
				t.AddIdentifier(colName, colValue)
	def CloseTable (self, tableName, statementList):
		for t in self.tlist:
			if t.name == tableName:
				statementList.append(t.createInsert())
				self.tlist.remove(t)
				del t			
		

class Table:
	#The Table structure simulates a DB Table
	#It has a name, a parent, columns and values.
	#We have some specialized columns called identifiers. These start with the id, then add in the automated counters.
	#The table also maintains a list of counters for its children. This allows the children to call back to the parent and ask for the next number in that counter.

	def __init__(self, name, parent_name, table_list, tablePath):
		#initialization gets the parent
		#If there is a parent, the table first inherits the parent's identifiers
		#It then asks the parent for the next value in it's own identifier and adds that to the identifier list.
		#I could rewrite the later half as a function going through the TableList and it may be more correct, but this works well enough
		self.name = name
		self.columns=[]
		self.identifiers=[]
		self.counters =[]
		self.parent_name=parent_name
		if parent_name is not None:
			for table in table_list.tlist:
				if table.name == parent_name:
					parent = table
					for identifier in parent.GetIdentifiers():
						self.AddIdentifier(identifier.name, identifier.value)
					new_id = parent.GetCounter(tablePath)
					self.AddIdentifier(new_id.name, new_id.value)
	def AddCol(self,colName, colValue):
		#Simply adds a column name, value name pair to the list to be output, called via TableList.AddCol
		newcol = Column(colName, colValue)
		self.columns.append(newcol)
	def AddIdentifier(self,colName, colValue):
		#Adds a new column, value to the identifier list. Can be called via TableList.AddIdentifier, but that should only happen at the start of a record
		newcol = Column(colName, colValue)
                self.identifiers.append(newcol)
	def GetCounter(self, name):
		#This accepts a counter name and returns the next value for that counter
		#This would be invoked by a Table's children (see in __init__).
		#The parent Table will look for the name in the list of Counters
		# if found, add 1 and report the [name, number]
		#else, create a new Counter in the list and report [name, 1]
		ctr_id = ctr_dict[name+ "/ctr_id"].split(":",1)[1]
		for counter in self.counters:
			if counter.name == ctr_id:
				counter.value = counter.value + 1
				return(counter)
		newcounter = Column(ctr_id,1)
		self.counters.append(newcounter)
		return(newcounter)

	def GetIdentifiers(self):
		#This returns the set of identifiers for the table, used when a child table is created to copy down the identifiers for the foreign key
		return(self.identifiers)
			
	def PrintCols(self):
		#Testing only
		print(len(self.columns))
		for col in self.columns:
			print(col.name)
			print(col.value)

	def createInsert(self):
		#Generates the insert statement for this table.
		#This is invoked when the table is closed. It goes through first the identifier list, then the column list and creates the insert statement.
		#The statement string is returned. This is then pushed onto a stack.
		colList = ""
		valList = ""
		for col in self.identifiers:
                        if colList <> "":
                                colList = colList + ","
                                valList = valList + ","
                        colList = colList + table_quote + col.name + table_quote
                        valList = valList + str(col.value)
		for col in self.columns:
			if colList <> "":
				colList = colList + ","
				valList = valList + ","
			colList = colList + table_quote + col.name + table_quote
			valList = valList + value_quote + db_string(col.value) + value_quote
		statement = "INSERT INTO %s%s%s (%s) VALUES (%s);" % (table_quote, self.name, table_quote, colList, valList)
		return(statement)
		

class Column:
	#A simple storage for a column in a table entry. Name and value.
	def __init__(self, name, value):
		self.name=name
		self.value=value


#set up strings for DB insertion, copied from the old parser, may vary by DB software?
def db_string(s):
	if s is not None:
		return str(s).replace("'","''").replace('\\', '\\\\')
	else:
		return "NULL"

def getXmlFiles(directory, recurse):
	filelist=[]
	if recurse:
		for root, _, files in os.walk(directory):
			for file in files:
				if file.endswith('.xml'):
					filelist.append(os.path.join(root, file))
	else:
		for filename in sorted(os.listdir(directory)):
			if filename.endswith('.xml'):
				filelist.append(os.path.join(directory, filename))
	filelist.sort()

	return filelist

if __name__ == "__main__":
	sys.exit(main())
