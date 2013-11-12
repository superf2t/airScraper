import MySQLdb
from database.settings import Settings

settings=Settings()

db=MySQLdb.connect(host=settings.host,user=settings.user,passwd=settings.password,db=settings.db)

class BaseDB(object):


    def __init__(self):
        q_s="show columns from %s;"%self.__class__.__name__
        self.attribs=[i[0] for i in self.commit(q_s)[1:]]
    def fetch(self,**kvargs):
        q=" AND ".join(["%s=%s"%(key,value) if str(value).isdigit() else "%s='%s'"%(key,value) for key,value in kvargs.iteritems()])
        q_s='SELECT * from %s WHERE %s'%(self.__class__.__name__,q)
        return self.commit(q_s)


    def commit(self,q_s):
        cur=db.cursor()
        cur.execute(q_s)
        id=cur.lastrowid
        db.commit()
        if "INSERT" in q_s:
            return id
        return cur.fetchall()

    def insert_without_commit(self,*data):
        q_s="INSERT INTO %s VALUES(null,%s);"%(self.__class__.__name__,",".join([db.escape_string(i) if  i.isdigit() or 'NOW()'  in i else  "'%s'"%db.escape_string(i.encode('ascii','ignore'))  for i in data]))
        cur=db.cursor()
        cur.execute(q_s)
        id=cur.lastrowid
        if "INSERT" in q_s:
            return id

    def commit_only(self):
        db.commit()

    def insert(self,*data):
        "data is a list or tuple"
        q_s="INSERT INTO %s VALUES(null,%s);"%(self.__class__.__name__,",".join([db.escape_string(i) if  i.isdigit() or 'NOW()'  in i else  "'%s'"%db.escape_string(i.encode('ascii','ignore'))  for i in data]))

        return self.commit(q_s)


    def get_attributes(self):
        return self.attribs

    def update(self,data,where='True'):
        """ data is a map of keys and data"""
        q=",".join(["%s=%s"%(key,value) if str(value).isdigit() else "%s='%s'"%(key,db.escape_string(value)) for key,value in data.iteritems()])
        q_s="UPDATE %s SET %s WHERE %s;"%(self.__class__.__name__,q,where)
        return self.commit(q_s)


class calender(BaseDB):
    pass

class location(BaseDB):
    pass


class listing(BaseDB):
    """Last item is name from location"""
    def insert_with_location(self,*args):
        loc=location()
        id_location=loc.insert(args[-1],"0")
        _list=list(args)
        _list[-3]=str(int(id_location))
        id_listing=self.insert(*_list[:-1])
        self.update({'location_id':id_location},'id=%s'%id_listing)
        loc.update({'parent_id':id_listing},'id=%s'%id_location)

    def search_and_insert_with_location(self,*ar):

        if len(self.fetch(airbnb_id=ar[0]))<1:
                self.insert_with_location(*ar)
        else:
            p_id=self.fetch(airbnb_id=ar[0])[0][0]
            loc=location()
            if len(loc.fetch(parent_id=p_id))<1:
                loc.insert(ar[-1],str(p_id))










