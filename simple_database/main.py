import os
from datetime import date
import json

from simple_database.exceptions import ValidationError
from simple_database.config import BASE_DB_FILE_PATH

# Handle date objects not being json friendly
class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, date):
            return o.isoformat()
        return json.JSONEncoder.default(self, o)

class RowEntry(object):
    def __init__(self, **kwargs):
        for k in kwargs.keys():
            setattr(self, k, kwargs[k])

class Table(object):
    
    VALIDATION_ERROR_FORMAT = 'Invalid type of field "{}": Given "{}", expected "{}"'

    def __init__(self, db, name, columns=None):
        self.db = db
        self.name = name
        self.columns = columns
        self.path = BASE_DB_FILE_PATH + db.name + os.sep + name
        self._read()

    def insert(self, *args):
        row_data = {}
        # verify no. input fields matches no. colums
        if not len(args) == len(self.columns):
            raise ValidationError('Invalid amount of field')
        for i, col in enumerate(self.columns):
            field_name = col['name']
            field_type = col['type']
            input_type = type(args[i]).__name__
            if field_type == input_type:
                row_data[field_name] = args[i]
            else:
                raise ValidationError(self.VALIDATION_ERROR_FORMAT.format(field_name, input_type, field_type))
        self.rows.append(row_data)
        self._write()

    def query(self, **kwargs):
        search_parameters = list(kwargs.keys())
        search_results = [x for x in self.rows]
        while len(search_parameters) > 0:
            s = search_parameters.pop()
            search_results = [row for row in search_results if row[s] == kwargs[s]]
        query_results = []
        for result in search_results:
            query_results.append(RowEntry(**result))
        return query_results

    def all(self):
        for row in self.rows:
            yield RowEntry(**row)

    def count(self):
        return len(self.rows)

    def describe(self):
        return self.columns
    
    @classmethod
    def create(cls, db, name, columns):
        data = {'columns': columns}
        path = db.path + os.sep + name
        with open(path, 'w') as file:
            json.dump(data, file, cls=DateTimeEncoder)
        return cls(db, name)
        
    
    def _read(self):
        with open(self.path) as file:
            data = json.load(file)
        self.columns = data['columns']
        self.rows = data.get('rows', [])
    
    def _write(self):
        data = {'columns': self.columns, 'rows': self.rows}
        with open(self.path, 'w') as file:
            json.dump(data, file, cls=DateTimeEncoder)


class DataBase(object):
    def __init__(self, name):
        self.name = name
        self.path = BASE_DB_FILE_PATH + name
        for table in self.show_tables():
            setattr(self, table, Table(self, table))

    @classmethod
    def create(cls, name):
        if not os.path.exists(BASE_DB_FILE_PATH):
            os.mkdir(BASE_DB_FILE_PATH)
        if not os.path.exists(BASE_DB_FILE_PATH + name):
            os.mkdir(BASE_DB_FILE_PATH + name)
        else:
            raise ValidationError('Database with name "{}" already exists.'.format(name))
        return cls(name)

    def create_table(self, table_name, columns):
        #check to make sure file does not exist
        table_path = self.path + os.sep + table_name
        if not os.path.isfile(table_path):
            new_table = Table.create(self, table_name, columns)
            setattr(self, table_name, new_table)
            
        else:
            raise ValueError("Table already exists.")
            

    def show_tables(self):
        return os.listdir(self.path)


def create_database(db_name):
    """
    Creates a new DataBase object and returns the connection object
    to the brand new database.
    """
    DataBase.create(db_name)
    return connect_database(db_name)


def connect_database(db_name):
    """
    Connectes to an existing database, and returns the connection object.
    """
    return DataBase(name=db_name)
