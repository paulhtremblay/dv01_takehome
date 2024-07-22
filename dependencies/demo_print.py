import glob
from prettytable import PrettyTable

def _add_row(d:dict, fields:list, table:object):
    l = []
    for i in fields:
        l.append(d.get(i))
    table.add_row(l)

def _add_file_rows(path:str, fields: list, table: object):
    with open(path, 'r') as read_obj:
        for line in read_obj:
            d = eval(line)
            _add_row(d = d, fields = fields, table = table)


def pretty_output(path:str):
    table = PrettyTable()
    fields = ['total_issued', 'current', 'late', 'charged', 
            'principal_payments_received', 'interest_payments_received', 'avg_interest']
    table.field_names =  fields
    for i in glob.glob(f'{path}/*'):
        _add_file_rows(path = i, fields = fields, table = table)
    print(table)


if __name__ == '__main__':
    pretty_output('data_out')
