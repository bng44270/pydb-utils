import pymysql.cursors

class MySQLDB:
  def __init__(self,host,user,password,db):
    self.CONN = pymysql.connect(host=host,user=user,password=password,db=db)
    self.SCH = pymysql.connect(host=host,user=user,password=password,db='information_schema')
    
  def __del__(self):
    self.CONN.close()
    self.SCH.close()
  
  def Select(self,table,fieldspec=[],where=[]):
    returnValue = []
    
    with self.CONN.cursor() as cursor:
      query = 'SELECT {} FROM {} {};'.format('*' if len(fieldspec)== 0 else ','.join(fieldspec),table,'' if len(where) == 0 else ('WHERE ' + ' AND '.join([('{}=\'{}\''.format(a[0],a[1]) if isinstance(a[1],str) else '{}={}'.format(a[0],a[1])) for a in where])))
      cursor.execute(query)
      result = cursor.fetchall()
      
      for row in result:
        if len(fieldspec) == 0:
          returnValue.append(row)
        else:
          rowobj = {}
          for i in range(0,len(row)):
            rowobj[fieldspec[i]] = row[i]
          returnValue.append(rowobj)
      
    return returnValue
  
  def Insert(self,table,fields,values):
    with self.CONN.cursor() as cursor:
      query = 'INSERT INTO {}{} VALUES {};'.format(table,'' if len(fields)==0 else ('(' + ','.join(fields) + ')'),','.join(['({})'.format(','.join([('\'{}\''.format(a) if isinstance(a,str) else '{}'.format(a)) for a in row])) for row in values]))
      cursor.execute(query)
      self.CONN.commit()
      
  def Delete(self,table,where=[]):
    with self.CONN.cursor() as cursor:
      query = 'DELETE FROM {} WHERE {};'.format(table,'1' if len(where)==0 else ' AND '.join([('{}=\'{}\''.format(a[0],a[1]) if isinstance(a[1],str) else '{}={}'.format(a[0],a[1])) for a in where]))
      cursor.execute(query)
      self.CONN.commit()
      
  def Schema(self,table):
    fieldList = []
    with self.SCH.cursor() as cursor:
      query = 'SELECT column_name FROM columns WHERE table_name=\'{}\';'.format(table)
      cursor.execute(query)
      result = cursor.fetchall()
      
      for field in result:
        fieldList.append(field[0])
    
    return fieldList

class MySQLTable(MySQLDB):
  def __init__(self,host,user,password,db,table):
    super().__init__(host,user,password,db)
    self.TABLE = table
    
  def __del__(self):
    super().__del__()
    
  def Select(self,fieldspec=[],where=[]):
    return super().Select(self.TABLE,fieldspec,where)
    
  def Insert(self,fields,value):
    super().Insert(self.TABLE,fields,value)
    
  def Delete(self,where=[]):
    super().Delete(self.TABLE,where);
    
  def Schema(self):
    return super().Schema(self.TABLE)