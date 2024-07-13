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

    Derived classes must provide a Run.
    
    The Run method that must:

      - Accept two arguments:  a query and returned row count.
        If returned row count is omitted or is zero, all rows are returned
    
    The Run method should adhere to the following convention(s):

      - Return an array of rows (even if a single row is returned)
  """
  @abstractclassmethod
  def Run(self,q,count=0):
    pass

class MySqlConn(DbConnection):
  """
    Usage:
      
      # Establish DB Conection
      dbconn = MySQLConn("localhost","username","password","customer_db")
    
    Database connection closed when object is deconstructed
  """
  def __init__(self,host,user,password,db):
    self.CONN = pymysql.connect(host=host,user=user,password=password,db=db)
    self.SCH = pymysql.connect(host=host,user=user,password=password,db='information_schema')
  
  def __del__(self):
    self.CONN.close()
    self.SCH.close()
  
  @DbConnection.Run
  def Run(self,q,count=0):
    """
      # Run SQL query (returns results of fetchall() function)
      results = dbconn.Run("SELECT * FROM namelist;")
      
      # Run SQL query with record count (returns results of fetchmany() function)
      results = dbconn.Run("SELECT * FROM namelist;",10)
      
      # Run SQL query that updates data (INSERT, UPDATE, ALTER, CREATE, DELETE, or DROP)
      # NOTE:  This will run commit() function
      dbconn.Run("INSERT INTO namelsit(name) VALUES (\"bob\");")
    """
    cmd = q.split(' ')[0]
    
    with self.CONN.cursor() as cursor:
      cursor.execute(q)
      
      if cmd.upper() in ['INSERT','UPDATE','ALTER','CREATE','DELETE','DROP']:
        self.CONN.commit()  
      else:
        return cursor.fetchall() if count == 0 else cursor.fetchmany(count)
  
  def TableSchema(self,t):
    """
      # Return a list containing fields in a table
      schema = dbconn.TableSchema("contacts")
    """
    fieldlist = []
    
    with self.SCH.cursor() as cursor:
      cursor.execute(f'SELECT column_name FROM columns WHERE table_name=\'{t}\';')

      fieldlist = [a[0] for a in cursor.fetchall()]
      
      return fieldlist

class SqliteConn(DbConnection):
  """
    Usage:
      
      # Establish DB Conection
      dbconn = SqliteConn("/path/to/sqlite.db")
      
      # Establish DB connection with custom PRAGMA directives
      dbconn = SqliteConn('/path/to/file.db',['foreign_key = ON'])
    
    Database connection closed when object is deconstructed
  """
  def __init__(self,file,pragma_list=[]):
    self.db = sqlite3.connect(file)
    for pragma in pragma_list:
      self.db.cursor().execute('PRAGMA {};'.format(pragma))
  
  def __del__(self):
    self.db.close()
  
  def Run(self,q,count=0):
    """
      # Run SQL query (returns results of fetchall() function)
      results = dbconn.Run("SELECT * FROM namelist;")
      
      # Run SQL query with record count (returns results of fetchmany() function)
      results = dbconn.Run("SELECT * FROM namelist;",10)
      
      # Run SQL query that updates data (INSERT, UPDATE, ALTER, CREATE, DELETE, or DROP)
      # NOTE:  This will run commit() function
      dbconn.Run("INSERT INTO namelsit(name) VALUES (\"bob\");")
    """
    cmd = q.split(' ')[0]
    
    results = self.db.cursor().execute(q)
    
    if cmd.upper() in ['INSERT','UPDATE','ALTER','CREATE','DELETE','DROP']:
      self.db.commit()
    else:
      return results.fetchall() if count == 0 else results.fetchmany(count)