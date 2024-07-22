from typing import Generator

def get_schema(schema:str)-> dict:
    d = {}
    for counter, i in enumerate(schema.split(',')):
        name = i.split(':')[0].strip()
        d[counter] = name
    return d

def to_the_dict(l:list, schema:dict) -> Generator:
    d = {}
    for counter, i in enumerate(l):
        field_name = schema[counter]
        d[field_name] = i

    yield d
