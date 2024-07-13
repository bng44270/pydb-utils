#########################################
#
# NOTE:  This has been replaced by MySqlConn in dbconnection.py 
#
# (Another) Quick and Dirty Wrapper for MySQL Library
#
# For usage information type:
#
#    help(MySqlConn)
#
#########################################

import pymysql.cursors

class MySqlConn:
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

      results = cursor.fetchall()
      
      for thisfield in results:
        fieldlist.append(thisfield[0])
      
      return fieldlist