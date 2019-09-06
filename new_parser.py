#!/usr/bin/env python3

''' new_parser.py

    A generic XML to SQL query parser, designed to convert an XML file into a
    set of queries based on a configuration file.

    Based on https://github.com/cns-iu/generic_parser
'''

import argparse
import csv
import datetime
import logging
import os
from collections import defaultdict, OrderedDict
from pathlib import Path

from lxml import etree


class Parser:

    def __init__(self, args):

        # These dictionaries are populated by Parser.read_config()
        self.table_dict = {}
        self.value_dict = {}
        self.ctr_dict = {}
        self.attrib_dict = {}
        self.attrib_defaults = defaultdict(dict)
        self.file_number_dict = {}

        self.xml_files = Path(args.xml_source)

        if self.xml_files.is_file():
            self.xml_files = [self.xml_files]
        elif self.xml_files.is_dir():
            self.xml_files = self.xml_files.glob(f'{"**/" if args.recurse else ""}*.xml')
        else:
            logging.fatal('specified input is invalid')
            exit(1)

        self.output_dir = args.output

        self.namespace = args.namespace

        # root tag can be empty, but rec and id need to be present
        self.root_tag = args.parent
        self.rec_tag = f'{self.namespace}{args.record}'
        self.id_tag = f'{self.namespace}{args.identifier}'

        # convert the file number sheet to a dictionary for speedy lookup
        if args.file_number_sheet is not None:
            logging.info(f'Parsing file numbers from {args.file_number_sheet}')
            # convert this into a dictionary
            with open(args.file_number_sheet, 'r') as _fh:
                self.file_number_lookup = dict(csv.reader(_fh))
        else:
            self.file_number_lookup = {}

        # STEP 2 - Convert the config file into lookup tables
        # write lookup tables for table creation, counters, value and
        #  attribute parsing
        self.read_config(args.config_file)


    def parse(self):
        # STEP 3 - Parse the file(s)
        # now that we have lookups, we start with the files themselves

        for filepath in self.xml_files:
            self.parse_file(filepath)


    def parse_file(self, filepath):

        logging.info(f'Parsing file: {filepath}')
        outputtarget = os.path.join(self.output_dir, filepath.with_suffix('.sql').name)
        logging.info(f'Writing to {outputtarget}')


        logging.info("Start time: %s" % datetime.datetime.now())
        output = open(outputtarget, "w")
        output.write("BEGIN;\n")

        # get file number
        file_number = self.file_number_lookup.get(filepath.name, -1)

        # the root of what we're processing may not be the root of the file
        #  itself
        # we need to know what portion of the file to process
        # we assume that there is only one of these, but it need not
        #  necessarily be true, I think.

        parser_args = {
            'remove_comments': True,
            'events': ('start', 'end')
        }

        if self.root_tag is None:
            # if there is no root tag, then we've only got one record and we process everything
            process = True
            parser_args['tag'] = self.rec_tag
        else:
            # we need to split this into a list of tags by "/".
            root_path = [f'{self.namespace}{s}' for s in self.root_tag.split("/")]
            process = False

        # The recover ability may or may not be available based on the version
        #  of lxml installed. Try to use it, but if not, go without
        try:
            parser = etree.iterparse(
                str(filepath.absolute()), recover=True, **parser_args)
        except:
            parser = etree.iterparse(tr(filepath.absolute()), **parser_args)

        path_note = []
        for event, elem in parser:
            # Here we keep an eye on our path.
            # If we have a root path defined, then we build a path as we go
            # If we are opening a tag that matches the root path, then we set processing to true
            # If we close a tag that matches the root path, then we set processing to false

            # if there is no root path, then we set process to true earlier and just leave it that way
            if self.root_tag is not None:
                if event == 'start':
                    # add the new element to the current path
                    path_note.append(elem.tag)
                    # if the path matches the root path, then we have reached
                    #  an area of interest, set processing to true
                    if path_note == root_path:
                        process = True
                elif event == 'end':
                    # if the path equals the root path, then we are leaving an
                    #  area of interest, set processing to false
                    if path_note == root_path:
                        process = False
                    # remove the last element from the current path
                    path_note.pop()

            # iteratively parse through the XML, focusing on the tag that
            #  starts a record
            # pass over things outside the processing area. Only process end
            #  tags.
            if event == 'end' and process is True:
                if elem.tag == self.rec_tag:

                    # you've got a record, now parse it

                    table_list = TableList(self)
                    statement_list = []
                    path = self.rec_tag

                    table_path = f'{path}/'
                    file_number_path = f'{path}/file_number'
                    valuepath = f'{path}/'

                    # get the core table name from the lookup
                    core_table_name = self.table_dict[table_path]

                    # create the core table
                    table_list.add_table(core_table_name, None, path)

                    # get the primary key
                    # the head tag may be the identifier, if so, just grab it,
                    #  otherwise, seek it out

                    if self.id_tag != self.rec_tag:
                        id_seek = self.id_tag
                        id_node = elem.find(id_seek)
                        id_value = f"'{id_node.text}'"
                    else:
                        id_value = f"'{elem.text}'"

                    # set the primary key
                    table_list.add_identifier(core_table_name, 'id', id_value)

                    # see if this table needs a file number
                    file_number_name = \
                        self.file_number_dict.get(file_number_path, False)
                    if file_number_name:
                        table_list.add_col(
                            file_number_name.split(":", 1)[0],
                            file_number_name.split(":", 1)[1], file_number)

                    attrib_seen = set()

                    # process the attributes
                    for attrib_name, attrib_value in elem.attrib.items():
                        attribpath = f'{path}/{attrib_name}'
                        if attribpath in self.attrib_dict:
                            table_name, col_name = \
                                self.attrib_dict[attribpath].split(":")[:2]
                            table_list.add_col(
                                table_name, col_name, str(attrib_value))
                            attrib_seen.add(attrib_name)

                    # process default attribute values
                    for attrib_name, attrib_value_all in self.attrib_defaults.get(path, {}).items():
                        if attrib_name not in attrib_seen:
                            table_name, col_name, attrib_value = attrib_value_all.split(":")[:3]
                            table_list.add_col(table_name, col_name, str(attrib_value))

                    # SIMON: where is node supposed to come from??
                    #        the branch seems never to be executed?
                    # process the value
                    if valuepath in self.value_dict:
                        if node.text is not None:
                            table_list.add_col(
                                value_dict[valuepath].split(":", 1)[0],
                                value_dict[valuepath].split(":", 1)[1],
                                str(node.text)
                            )

                    # process the children
                    for child in elem:
                        self.parse_node(child, path, table_list, core_table_name, statement_list)

                    # close the primary table
                    table_list.close_table(core_table_name, statement_list)

                    # write out the statements in reverse order to ensure key compliance

                    data = ""
                    for statement in reversed(statement_list):
                        data = data + (str(statement) + "\n")

                    output.write(data)

                    # clear memory

                    output.flush()
                    elem.clear()
                    # finished individual record

            if elem.getparent() is None and event == "end":
                break
                # some versions of lxml run off the end of the file. This
                #  forces the for loop to break at the root.


        # reenable unique constraint checking and close the output file
        # if db_mode == "mysql":
        #     output.write("SET unique_checks=1;\n")
        #     output.write("SET autocommit=1;\n")
        # if single_trans:
        output.write("COMMIT;\n")
        output.close()
        print("End time: %s" % datetime.datetime.now())

    def parse_node(self, node, path, table_list, last_opened, statement_list):
        # recursive node parser
        # given a node in a tree known not to be the record tag, parse it and
        #  its children

        # first, update the path from parent, for use in lookups
        if node.tag.find("}") > -1:
            tag = node.tag.split("}", 1)[1]
        else:
            tag = node.tag

        newpath = f'{path}/{tag}'

        # see if we need a new table, make sure children inherit the right parent
        table_path = f'{newpath}/'
        valuepath = f'{newpath}/'

        # See if this tag requires a new table
        if table_path in self.table_dict:
            new_table = True
            table_name = self.table_dict[table_path]
            table_list.add_table(table_name, last_opened, newpath)
        else:
            new_table = False
            table_name = last_opened

        # SIMON: file_number is not available here?
        #        another branch that is never used?
        # See if this tag calls for a file number
        if newpath in self.file_number_dict:
            file_number_name = self.file_number_dict[newpath]
            table_list.add_col(
                file_number_name.split(":", 1)[0],
                file_number_name.split(":", 1)[1],
                file_number
            )

        # process attributes
        attrib_seen = set()
        for attrib_name, attrib_value in node.attrib.items():
            attribpath = f'{newpath}/{attrib_name}'
            if attribpath in self.attrib_dict:
                table_name, col_name = \
                    self.attrib_dict[attribpath].split(":")[:2]
                table_list.add_col(table_name, col_name, str(attrib_value))
                attrib_seen.add(attrib_name)

        # process default attribute values
        for attrib_name, attrib_value_all in self.attrib_defaults.get(newpath, {}).items():
            if attrib_name not in attrib_seen:
                table_name, col_name, attrib_value = \
                    attrib_value_all.split(":")[:3]
                table_list.add_col(table_name, col_name, str(attrib_value))

        # process value
        if valuepath in self.value_dict:
            if node.text is not None:
                table_list.add_col(
                    self.value_dict[valuepath].split(":", 1)[0],
                    self.value_dict[valuepath].split(":", 1)[1],
                    str(node.text)
                )

        # process children
        for child in node:
            self.parse_node(
                child, newpath, table_list, table_name, statement_list)

        # if we created a new table for this tag, now it's time to close it.
        if new_table is True:
            table_list.close_table(table_name, statement_list)

    def read_config(self, config_file):

        def update_lookup_tables(node, path=''):

            # This recursive function will go through the config file, reading
            #  each tag and attribute and create the needed lookup tables
            # All tags and attributes are recorded by full path, so name
            #  reusage shouldn't be a problem

            newpath = f'{path}{node.tag}/'

            # write the value lookup for the tag
            if node.text is not None:
                if str(node.text).strip() != '':
                    self.value_dict[f'{self.namespace}{newpath}'] = node.text

            # go through the attributes in the config file
            # specialized ones like table and ctr_id go into their own lookups,
            #  the rest go into the attribute lookup
            for attrib_name, attrib_value_all in node.attrib.items():
                attrib_value = ':'.join(attrib_value_all.split(':')[:2])

                attrib_path = f'{self.namespace}{newpath}{attrib_name}'
                if attrib_name == "table":
                    self.table_dict[f'{self.namespace}{newpath}'] = attrib_value
                elif attrib_name == "ctr_id":
                    self.ctr_dict[f'{self.namespace}{newpath}'] = attrib_value
                elif attrib_name == "file_number":
                    self.file_number_dict[attrib_path] = attrib_value
                else:
                    self.attrib_dict[attrib_path] = attrib_value

                    # Providing a third tuple item specifies the default value
                    #  for that attribute
                    # If the attribute isn't found in the data, use the default
                    #  value instead.
                    if len(attrib_value_all.split(':')) == 3:
                        self.attrib_defaults[
                            f'{self.namespace}{newpath}'.strip('/')][attrib_name]\
                             = attrib_value_all

            # Now recurse for the children of the node
            for child in node:
                update_lookup_tables(child, newpath)

        update_lookup_tables(etree.parse(open(config_file)).getroot())


class TableList:
    """
    The TableList is the memory structure that stores the data as we read it
     out of XML
    This is the only way that we handle the Tables that we're creating during
     the main process.
    We can never have more than one instance of a table with the same name
    When a tag that needs a table opens, we call add_table.
    add_identifier should only be needed for the master table. Identifiers are
     added automatically after that.
    add_col is used for each value that we detect
    When a tag that created a table closes, we call close_table for that table.
     This kicks the insert statement out to the stack and frees up that table
     name if needed again.
    """

    def __init__(self, parser):
        self.parser = parser
        self.tlist = []

    def add_table(self, table_name, parent_name, table_path):
        t = Table(table_name, parent_name, self, table_path, self.parser)
        self.tlist.append(t)

    def add_col(self, table_name, col_name, col_value):
        for t in self.tlist:
            if t.name == table_name:
                t.add_col(col_name, col_value)
                return

    def add_identifier(self, table_name, col_name, col_value):
        for t in self.tlist:
            if t.name == table_name:
                t.add_identifier(col_name, col_value)
                return

    def close_table(self, table_name, statement_list):
        for t in self.tlist:
            if t.name == table_name:
                statement_list.append(t.create_insert())
                self.tlist.remove(t)
                del t
                return


class Table:
    """
    The Table structure simulates a DB Table
    It has a name, a parent, columns and values.
    We have some specialized columns called identifiers. These start with the
     id, then add in the automated counters.
    The table also maintains a list of counters for its children. This allows
     the children to call back to the parent and ask for the next number in
     that counter.
    """

    # SIMON: this is the place to subclass PostgresTable, MySQLTable...
    def __init__(self, name, parent_name, table_list, table_path, parser):
        # initialization gets the parent
        # If there is a parent, the table first inherits the parent's identifiers
        # It then asks the parent for the next value in it's own identifier and adds
        #  that to the identifier list.
        # I could rewrite the later half as a function going through the TableList and
        #  it may be more correct, but this works well enough

        self.name = name
        self.columns = OrderedDict()
        self.identifiers = OrderedDict()
        self.counters = defaultdict(int)
        self.parent_name = parent_name
        self.parser = parser

        self.table_quote = '"'
        self.value_quote = '\''

        if parent_name is not None:
            for table in table_list.tlist:
                if table.name == parent_name:
                    parent = table
                    for identifier_name, identifier_value in parent.identifiers.items():
                        self.add_identifier(identifier_name, identifier_value)
                    new_id, new_id_ct = parent.get_counter(table_path)
                    self.add_identifier(new_id, new_id_ct)

    def db_string(self, s):
        if s is None:
            return 'NULL'
        return str(s).replace("'", "''").replace('\\', '\\\\').replace('\n', '')

    def add_col(self, col_name, col_value):
        # Simply adds a (col_name, col_value) pair to the list to be output, called via TableList.add_col
        self.columns[col_name] = col_value

    def add_identifier(self, col_name, col_value):
        # Adds a new column, value to the identifier list. Should only happen at the start of a record
        self.identifiers[col_name] = col_value

    def get_counter(self, name):
        # This accepts a counter name and returns the next value for that counter
        # This would be invoked by a Table's children (see in __init__).
        # The parent Table will look for the name in the list of Counters
        #  if found, add 1 and report the [name, number]
        #  else, create a new Counter in the list and report [name, 1]
        _table, ctr_id = self.parser.ctr_dict[f'{name}/'].split(":", 1)
        self.counters[ctr_id] += 1
        return ctr_id, self.counters[ctr_id]

    def create_insert(self):

        col_list = ','.join([
            f'{self.table_quote}{col_name}{self.table_quote}'
            for col_name in list(self.identifiers.keys()) + list(self.columns.keys())])
        val_list = ','.join(
            [f'{col_value}' for col_value in self.identifiers.values()] +
            [
                f'{self.value_quote}{self.db_string(col_value)}{self.value_quote}'
                for col_value in self.columns.values()
            ]
        )

        return (
            f'INSERT INTO {self.table_quote}{self.name}{self.table_quote} '
            f'({col_list}) VALUES ({val_list});')


def main():
    ''' Command-line entry-point. '''

    parser = argparse.ArgumentParser(
        description='Description: {}'.format(__file__))

    parser.add_argument(
        '-v', '--verbose', action='store_true', default=False,
        help='Increase verbosity')
    parser.add_argument(
        '-q', '--quiet', action='store_true', default=False,
        help='quiet operation')

    # -s REQUIRED, input file or directory
    parser.add_argument(
        '-x', '--xml-source', action='store', required=True,
        help='parse a single file')

    # -c REQUIRED, defines the configuration file mapping from XML to DB
    parser.add_argument(
        '-c', '--config-file', action='store', required=True,
        help='configuration file')

    # -o REQUIRED, can be either a directory, or if a single-file run, a file
    #    name.
    parser.add_argument(
        '-o', '--output', action='store', required=True,
        help='output file or directory')

    # -p optional, marks the container tag for a collection of records, would
    #    not be used for single record files
    parser.add_argument(
        '-p', '--parent', action='store',
        help='Name of the parent tag (tag containing the group of records')

    # -r REQUIRED, the tag that defines an individual record
    parser.add_argument(
        '-r', '--record', action='store', required=True,
        help='Name of the tag that defines a single record')

    # -n optional, if the XML has a namespace, give it here. Assumes a single
    #    namespace for the entire file
    parser.add_argument(
        '-n', '--namespace', action='store', help='Namespace of the XML file')

    # -i REQUIRED, the tag that gives the unique identifier for the record. If
    #    this is a direct child of the record root, just give the child name,
    #    otherwise, starting at that level, give the path.
    parser.add_argument(
        '-i', '--identifier', action='store', required=True,
        help='Name of the tag whose value contains the unique identifier for '
             'the record')

    # -l optional, ran out of good letters, required to use file numbers
    parser.add_argument(
        '-l', '--file-number-sheet', action='store',
        help='CSV file with the file name to file number lookup')

    # -m, database mode, a toggle between MySQL and PostgreSQL?
    parser.add_argument(
        '-m', '--database-mode', action='store',
        help='MySQL or Postgres, defaults to Postgres')

    # -s, wraps the entire file's output into a single transaction, good for
    #     speed if DB has an autocommit that you can't disable.
    parser.add_argument(
        '-s', '--single-trans', action='store_true',
        help='If true, will enable one transaction per file')

    # -z, gives the option to recurse through a directory as opposed to just
    #     reading the core output.
    parser.add_argument(
        '-z', '--recurse', action='store_true',
        help='If true and a directory is set, the parser will search '
        'subdirectories for XML files to parse as well, ignored for '
        'single file parse')

    args = parser.parse_args()

    log_level = logging.DEBUG if args.verbose else logging.INFO
    log_level = logging.CRITICAL if args.quiet else log_level
    logging.basicConfig(
        level=log_level,
        format='%(message)s'
    )

    parser = Parser(args)
    parser.parse()


if __name__ == '__main__':
    main()
