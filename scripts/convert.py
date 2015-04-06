#!/usr/bin/python3
import argparse


class Task(object):
    def __str__(self):
        return str(self.__dict__)

task = Task()

arg_parser = argparse.ArgumentParser(description='Dataset libFM convertor.')
arg_parser.add_argument('--input', required=True, help='Input data file')
arg_parser.add_argument('--target-column', required=True, help='Target column')
arg_parser.add_argument('--header', help='Is data file have header')
arg_parser.add_argument('--delete-column', default=None, help='Delete column')
arg_parser.add_argument('--offset', default=None, help='ID offset')
arg_parser.add_argument('--separator', default='::', help='File columns separator')
arg_parser.add_argument('--outmeta', help='Meta output')

arg_parser.parse_args(namespace=task)

input_files = task.input.split(',')
print(input_files)

id_counter = 0
if task.offset:
    id_counter = int(task.offset)

out_groups = None
if task.outmeta:
    out_groups = open(task.outmeta)

target_column = int(task.target_column)

delete_columns = []
if task.delete_column:
    delete_columns = task.delete_column.split(',')
delete_columns = [int(x) for x in delete_columns]

ids = {}
legend_files = {}
for cur_file_name in input_files:
    print('transforming "' + cur_file_name + '" to "' + cur_file_name + '.libfm"...')
    file_out = open(cur_file_name + '.libfm', 'w')
    num_triples = 0
    cur_file = open(cur_file_name, 'r')
    lines = [x.strip() for x in cur_file.read().splitlines() if x]
    if task.header:
        file_out.write(lines[0])
        del lines[0]
    for line in lines:
        data = line.split(task.separator)
        if len(data) < int(task.target_column):
            raise RuntimeError('Not enough values in line ' + str(lines.index(line)) +
                               ', expected at least ' + str(task.target_column) + ' elements')

        if not len(legend_files):
            for d in range(len(data)):
                if d != target_column and d not in delete_columns:
                    legend_files[d] = open(cur_file_name + '.leg-' + str(d), 'w')

        out_str = data[target_column]
        # says which column in the input a field corresponds to after "deleting"
        # the "delete_column", i.e. it is a counter over the #$data-field
        # in @data assuming that some of the columns have been deleted;
        # one can see this as the "group" id
        out_col_id = 0
        for d in range(len(data)):
            if d == target_column or d in delete_columns:
                continue

            # this id holds the unique id of $data[$i] (also w.r.t. its group)
            col_id = str(out_col_id) + ' ' + data[d]
            if col_id not in ids:
                ids[col_id] = id_counter
                legend_files[d].write(str(id_counter) + task.separator + str(data[d]) + '\n')
                if out_groups:
                    out_groups.write(str(out_col_id) + '\n')
                id_counter += 1
            libfm_id = ids[col_id]
            out_str += ' ' + str(libfm_id) + ':1'
            out_col_id += 1
        file_out.write(out_str + '\n')
    file_out.close()

for f in legend_files:
    legend_files[f].close()
if out_groups:
    out_groups.close()
