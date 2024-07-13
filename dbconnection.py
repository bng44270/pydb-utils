#########################################
# (Another) Quick and Dirty Wrapper for Database connection wrapper
#
# Currently provided classes:
#
#    MySqlConn
#    SqliteConn
#
# To implement new databases use the DbConnection abstract class
#
# All class documentation is accessible via the help() function
#
#########################################

import sqlite3
import pymysql.cursors
from abc import ABC, abstractclassmethod

class DbConnection(ABC):
  """
    Base Class for Database connections

    Derived classes must provide the following methods:
    
      - Run - executes SQL query
      - Tables - returns list of tables
      - TableFields - returns list of fields in specified table
    
    In method examples, the database connection instance is named dbconn
  """
  @abstractclassmethod
  def __init__(self,conn):
    """
      Setup database connection

      Accepts connection string as argument that adheres to documentation
      of parseconnectionstring method
    """
    pass
  
  @abstractclassmethod
  def __del__(self):
    """
      Close database connection
    """
    pass
  
  @abstractclassmethod
  def Run(self,q,count=0):
    """
      The Run method that must:

        - Accept two arguments:  a query and returned row count.
          If returned row count is omitted or is zero, all rows are returned
        
        - Return an array of rows (even if a single row is returned)

      # Run SQL query (returns results of fetchall() function)
      results = dbconn.Run("SELECT * FROM namelist;")
      
      # Run SQL query with record count (returns results of fetchmany() function)
      results = dbconn.Run("SELECT * FROM namelist;",10)
      
      # Run SQL query that updates data (INSERT, UPDATE, ALTER, CREATE, DELETE, or DROP)
      # NOTE:  This will run commit() function
      dbconn.Run("INSERT INTO namelsit(name) VALUES (\"bob\");")
    """
    pass
  
  @abstractclassmethod
  def Tables(self):
    """
      # Returns a list of tables in database
      tables = dbconn.Tables()
    """
    pass
  
  @abstractclassmethod
  def TableFields(self,t):
    """
      # Return a list containing fields in a specified table
      schema = dbconn.TableSchema("contacts")
    """
    pass
  
  def Schema(self):
    """
      Return dictionary containing database schema (tables and fields)

      {
        "table1" : ["field1","field2","field3"],
        "table2" : ["field4","field5","field6"] 
      }
    """
    return dict([(t,self.TableFields(t)) for t in self.Tables()])

  def parseconnectionstring(self,s):
    """
      Parse the connection string in constructor

      Format of conection string is "param=value" with pairs separated by a semicolon.

      Function returns a dictionary of parameters/values in connection string
    
    """
    return dict([(opt.split('=')[0],opt.split('=')[1]) for opt in s.split(';')])

class MySqlConn(DbConnection):
  """
    Usage:
      
      # Establish DB Conection
      dbconn = MySQLConn("host=localhost;user=username;password=password;db=customer_db")

  """
  def __init__(self,conn):
    conn_params = self.parseconnectionstring(conn)
    self.DB = conn_params['db']
    self.CONN = pymysql.connect(host=conn_params['host'],user=conn_params['user'],password=conn_params['password'],db=conn_params['db'])
    self.SCH = pymysql.connect(host=conn_params['host'],user=conn_params['user'],password=conn_params['password'],db='information_schema')
  
  def __del__(self):
    self.CONN.close()
    self.SCH.close()
  
  def Run(self,q,count=0):
    cmd = q.split(' ')[0]
    
    with self.CONN.cursor() as cursor:
      cursor.execute(q)
      
      if cmd.upper() in ['INSERT','UPDATE','ALTER','CREATE','DELETE','DROP']:
        self.CONN.commit()  
      else:
        return cursor.fetchall() if count == 0 else cursor.fetchmany(count)
  
  def Tables(self):
    tablelist = []

    with self.SCH.cursor() as cursor:
      cursor.execute(f'SELECT table_name from tables where table_schema=\'{self.DB}\';')

      tablelist = [a[0] for a in cursor.fetchall()]

      return tablelist

  def TableFields(self,t):
    fieldlist = []
    
    with self.SCH.cursor() as cursor:
      cursor.execute(f'SELECT column_name FROM columns WHERE table_name=\'{t}\';')

      fieldlist = [a[0] for a in cursor.fetchall()]
      
      return fieldlist

class SqliteConn(DbConnection):
  """
    Usage:
      
      # Establish DB Conection
      dbconn = SqliteConn("file=/path/to/sqlite.db")
    
    Database connection closed when object is deconstructed
  """
  def __init__(self,conn):
    conn_params = self.parseconnectionstring(conn)
    self.db = sqlite3.connect(conn_params['file'])
  
  def __del__(self):
    self.db.close()
  
  def Pragma(self,p):
    """
      # Optional pragma's may be defined as follows:
      dbconn.Pragma('foreign_key = ON')
    """
    self.db.cursor().execute(f"PRAGMA {p};")
  
  def Run(self,q,count=0):
    cmd = q.split(' ')[0]
    
    results = self.db.cursor().execute(q)
    
    if cmd.upper() in ['INSERT','UPDATE','ALTER','CREATE','DELETE','DROP']:
      self.db.commit()
    else:
      return results.fetchall() if count == 0 else results.fetchmany(count)
  
  def Tables(self):
    return [a[0] for a in self.Run('SELECT name from sqlite_master where type=\'table\';')]
  
  def TableFields(self,t):
    return [a[0] for a in self.Run(f'select name from pragma_table_info(\'{t}\');')]