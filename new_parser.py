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
        self.fields = defaultdict(list)

        self.tables = {}
        self.current_output_path = None

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
            self.file_number_lookup = False

        # STEP 2 - Convert the config file into lookup tables
        # write lookup tables for table creation, counters, value and
        #  attribute parsing
        self.read_config(args.config_file)


    def parse(self):
        # STEP 3 - Parse the file(s)
        # now that we have lookups, we start with the files themselves

        for filepath in self.xml_files:
            self.parse_file(filepath)

        for table in self.tables.values():
            table.close_table()


    def parse_file(self, filepath):


        logging.info(f'Parsing file: {filepath}')

        self.current_output_path = Path(self.output_dir) / filepath.stem
        self.current_output_path.mkdir(parents=True, exist_ok=True)
        logging.info(f'Writing files to: {self.current_output_path}')

        logging.info("Start time: %s" % datetime.datetime.now())

        # the root of what we're processing may not be the root of the file
        #  itself
        # we need to know what portion of the file to process
        # we assume that there is only one of these, but it need not
        #  necessarily be true, I think.

        parser_args = {
            'remove_comments': True,
            'recover': True,
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

        parser = etree.iterparse(str(filepath.absolute()), **parser_args)

        path_note = []
        for event, node in parser:
            # Here we keep an eye on our path.
            # If we have a root path defined, then we build a path as we go
            # If we are opening a tag that matches the root path, then we set processing to true
            # If we close a tag that matches the root path, then we set processing to false

            # if there is no root path, then we set process to true earlier and just leave it that way
            if self.root_tag is not None:
                if event == 'start':
                    # add the new element to the current path
                    path_note.append(node.tag)
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
                if node.tag == self.rec_tag:

                    # you've got a record, now parse it
                    path = self.rec_tag

                    # get the core table name from the lookup
                    main_table_name = self.table_dict[path]

                    # open a record on the core table
                    main_record = self.get_record(main_table_name)

                    # get the primary key
                    # the head tag may be the identifier, if so, just grab it,
                    #  otherwise, seek it out

                    if self.id_tag != self.rec_tag:
                        id_seek = self.id_tag
                        id_node = node.find(id_seek)
                        id_value = f"'{id_node.text}'"
                    else:
                        id_value = f"'{node.text}'"

                    # set the primary key
                    main_record.add_identifier('id', id_value)

                    self.write_columns(node, path)

                    # process the children
                    for child in node:
                        self.parse_node(child, path, main_record)

                    main_record.close_record()
                    node.clear()

        logging.info("End time: %s" % datetime.datetime.now())


    def write_columns(self, node, path=None, record=None):
        if self.file_number_lookup:
            file_number_name = \
                self.file_number_dict.get(path, False)
            if file_number_name:
                file_number = self.file_number_lookup.get(filepath.name, -1)
                table_name, col_name = file_number_name.split(":", 1)
                self.get_record(table_name, path, record).add_col(col_name, file_number)

        # process attributes
        attrib_seen = set()
        for attrib_name, attrib_value in node.attrib.items():
            attribpath = f'{path}/{attrib_name}'
            if attribpath in self.attrib_dict:
                table_name, col_name = \
                    self.attrib_dict[attribpath].split(":")[:2]
                self.get_record(table_name, path, record).add_col(col_name, str(attrib_value))
                attrib_seen.add(attrib_name)

        # process default attribute values
        for attrib_name, attrib_value_all in self.attrib_defaults.get(path, {}).items():
            if attrib_name not in attrib_seen:
                table_name, col_name, attrib_value = attrib_value_all.split(":")[:3]
                self.get_record(table_name, path, record).add_col(col_name, str(attrib_value))

        # process value
        if path in self.value_dict:
            if node.text is not None:
                table_name, col_name = self.value_dict[path].split(":", 1)
                self.get_record(table_name, path, record).add_col(col_name, str(node.text))


    def parse_node(self, node, parent_path, parent_record):
        # recursive node parser
        # given a node in a tree known not to be the record tag, parse it and
        #  its children

        # first, update the path from parent, for use in lookups
        if node.tag.find("}") > -1:
            tag = node.tag.split("}", 1)[1]
        else:
            tag = node.tag

        path = f'{parent_path}/{tag}'

        # see if we need a new table, make sure children inherit the right parent
        # See if this tag requires a new table
        if path in self.table_dict:
            creating_record = True
            table_name = self.table_dict[path]
            record = self.get_record(table_name, path, parent_record)
        else:
            creating_record = False
            record = parent_record

        self.write_columns(node, path, record)

        # process children
        for child in node:
            self.parse_node(child, path, record)

        # if we created a new table for this tag, now it's time to close it.
        if creating_record is True:
            record.close_record()


    def read_config(self, config_file):

        def update_lookup_tables(node, path):

            # This recursive function will go through the config file, reading
            #  each tag and attribute and create the needed lookup tables
            # All tags and attributes are recorded by full path, so name
            #  reusage shouldn't be a problem



            # write the value lookup for the tag
            if node.text is not None:
                if str(node.text).strip() != '':
                    table, field = node.text.split(':')
                    self.fields[table].append(field)
                    self.value_dict[path] = node.text

            # go through the attributes in the config file
            # specialized ones like table and ctr_id go into their own lookups,
            #  the rest go into the attribute lookup
            for attrib_name, attrib_value_all in node.attrib.items():
                attrib_value = ':'.join(attrib_value_all.split(':')[:2])

                attrib_path = f'{path}/{attrib_name}'
                if attrib_name == "table":
                    self.table_dict[path] = attrib_value
                elif attrib_name == "ctr_id":
                    table, field = attrib_value.split(':')
                    self.fields[table].append(field)
                    self.ctr_dict[path] = attrib_value
                elif attrib_name == "file_number":
                    table, field = attrib_value.split(':')
                    self.fields[table].append(field)
                    self.file_number_dict[path] = attrib_value
                else:
                    table, field = attrib_value.split(':')
                    self.fields[table].append(field)
                    self.attrib_dict[attrib_path] = attrib_value
                    # Providing a third tuple item specifies the default value
                    #  for that attribute
                    # If the attribute isn't found in the data, use the default
                    #  value instead.
                    if len(attrib_value_all.split(':')) == 3:
                        self.attrib_defaults[
                            path][attrib_name] = attrib_value_all

            # Now recurse for the children of the node
            for child in node:
                update_lookup_tables(child, f'{path}/{child.tag}')

        root = etree.parse(open(config_file)).getroot()
        root_tag = root.tag
        path = f'{self.namespace}{root_tag}'
        update_lookup_tables(root, path)


    def get_record(self, table_name, table_path=None, parent_table=None):
        table = self.get_or_create_table(table_name, table_path, parent_table)
        if table.record_open:
            return table
        table.new_record()
        return table


    def get_or_create_table(self, table_name, table_path=None, parent_table=None):
        if table_name in self.tables:
            return self.tables[table_name]

        ctr_id = None
        if table_path is not None:
            _table, ctr_id = self.ctr_dict[table_path].split(":", 1)

        fields = ['id'] + self.fields[table_name]
        output_path = self.current_output_path.joinpath(f'{table_name}.sql')

        table = Table(
            table_name, fields, output_path, ctr_id, parent_table)
        self.tables[table_name] = table
        return table


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
    def __init__(self, name, fields, output_path, ctr_id=None, parent_table=None):
        # initialization gets the parent
        # If there is a parent, the table first inherits the parent's identifiers
        # It then asks the parent for the next value in it's own identifier and adds
        #  that to the identifier list.
        # I could rewrite the later half as a function going through the TableList and
        #  it may be more correct, but this works well enough

        self.name = name
        self.fields = fields
        self.ctr_id = ctr_id
        self.parent_table = parent_table

        self.columns = OrderedDict()
        self.identifiers = OrderedDict()
        self.counters = defaultdict(int)

        self.table_quote = '"'
        self.value_quote = '\''

        self.record_open = False

        if self.parent_table is not None:
            self.fields += [*self.parent_table.identifiers.keys()]

        self._fh = open(output_path, 'w')
        logging.debug(f'Opened {self._fh.name} for writing...')
        self._fh.write("BEGIN;\n")


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

    def new_record(self):
        # counters are unique per parent_id, so reset them here
        self.counters = defaultdict(int)

        if self.parent_table is not None:

            # copy identifiers from parent
            for identifier_name, identifier_value in self.parent_table.identifiers.items():
                self.add_identifier(identifier_name, identifier_value)

            # if this table needs a counter, add the next one off the rank
            if self.ctr_id is not None:
                new_id, new_id_ct = self.parent_table.get_counter(self.ctr_id)
                self.add_identifier(new_id, new_id_ct)

        self.record_open = True

    def get_counter(self, ctr_id):
        # This accepts a counter name and returns the next value for that counter
        # This would be invoked by a Table's children (see in __init__).
        # The parent Table will look for the name in the list of Counters
        #  if found, add 1 and report the [name, number]
        #  else, create a new Counter in the list and report [name, 1]
        self.counters[ctr_id] += 1
        return ctr_id, self.counters[ctr_id]

    def create_insert(self):

        col_list = ','.join([
            f'{self.table_quote}{col_name}{self.table_quote}'
            for col_name in [*self.identifiers.keys(), *self.columns.keys()]])
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

    def close_record(self):
        self._fh.write(self.create_insert() + '\n')
        self.columns = OrderedDict()
        self.identifiers = OrderedDict()
        self.record_open = False

    def close_table(self):
        assert not self.columns and not self.identifiers
        self._fh.write('COMMIT;\n')
        self._fh.close()


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

    # -o REQUIRED, can be either a directory, or if a single-file run, a file name.
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
