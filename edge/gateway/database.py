#

import pymysql

import gateway.runtime as rt
import gateway.metadata as md



class SQL:


    def __init__(self, gateway_database_connection = None, config_filepath = None, config_filename = None):

        self.gateway_database_connection = gateway_database_connection
        
        self.config_filepath = config_filepath
        self.config_filename = config_filename

        self.env = self.get_env()
        if self.gateway_database_connection is None: self.gateway_database_connection = self.env['GATEWAY_DATABASE_CONNECTION']

        self.conn = None


    def get_env(self):

        config = md.Configure(filepath = self.config_filepath, filename = self.config_filename)
        env = config.get()

        return env


    def connect_db(self):

        try:
            conn_data = self.gateway_database_connection
            self.conn = pymysql.connect(host = conn_data['host'], user = conn_data['user'], passwd = conn_data['passwd'], db = conn_data['db'], autocommit = True)
        except (pymysql.err.OperationalError, pymysql.err.Error) as e:
            rt.logging.exception(e)
            
        return self.conn


    def commit_transaction(self):

        try:
            self.conn.commit()
        except (pymysql.err.OperationalError, pymysql.err.Error) as e:
            rt.logging.exception(e)


    def close_db_connection(self):

        try:
            if self.conn is not None: 
                self.conn.close()
        except (pymysql.err.OperationalError, pymysql.err.Error, NameError) as e:
            rt.logging.exception(e)
