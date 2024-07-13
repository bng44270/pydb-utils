#########################################
#
# NOTE:  This has been replaced by SqliteConn in dbconnection.py 
#
# (Another) Quick and Dirty Wrapper for SQLite Library
#
# For usage information type:
#
#    help(SqliteConn)
#
#########################################

import sqlite3

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
  
  @DbConnection
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
    
    return results.fetchall() if count == 0 else results.fetchmany(count)