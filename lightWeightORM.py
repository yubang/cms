#coding:UTF-8

"""
一个轻量级orm框架，目标实现自动化使用数据库
第一期目标兼容mysql
@author:yubang
"""

import hashlib,time,re,json,datetime
import MySQLdb

"""
一个非常简单的缓存类，在生产环境不建议使用。
@author:yubang
"""
class Cache():
    "缓存类"
    def __init__(self):
        self.data={}
        self.dataTimeout={}
    def __del__(self):
        del self.data
        del self.dataTimeout
    
    def set(self,key,value,timeout=60):
        self.__deleteTimeoutData()#维护数据
        
        cacheKey=hashlib.md5(key).hexdigest()
        self.data[cacheKey]=value
        self.dataTimeout[cacheKey]=time.time()+timeout
        
    def get(self,key):
        cacheKey=hashlib.md5(key).hexdigest()
        if(self.data.has_key(cacheKey)):
            if(time.time()>self.dataTimeout[cacheKey]):
                del self.data[cacheKey]
                del self.dataTimeout[cacheKey]
                return None
            else:
                return self.data[cacheKey]
        else:
            return None
            
    def delete(self,key):
        cacheKey=hashlib.md5(key).hexdigest()
        if(self.data.has_key(cacheKey)):
            del self.data[cacheKey]
            del self.dataTimeout[cacheKey] 
            return True
        else:
            return False
            
    def __deleteTimeoutData(self):
        "删除过期数据，用于维护数据"
        keys=self.data.keys()
        t=time.time()
        for key in keys:
            if(t>self.dataTimeout[key]):
                #删除过期数据
                del self.data[key]
                del self.dataTimeout[key]

"""
一个简单的日记类，建议生产环境关闭日记输出~
"""                
class Log():
    "日记类"
    """
    @param type 日记输出类型
    @param filePath 日记输出文件路径（仅仅type2使用）
    @param level 日记等级
    type 0 关闭日记
    type 1 表示控制台输出
    type 2 表示文件输出
    @author:yubang
    """
    def __init__(self,type=0,filePath="sql.log",level=0):
        "构造方法"
        self.type=type
        self.filePath=filePath
    def log(self,data,level):
        "记录日记"
        if(self.type==1):
            print unicode(u"系统日记："),data
        elif(self.type==2):
            fp=open(filePath,'a')
            fp.write(data)
            fp.write('\n')
            fp.close()
            
"""
orm类，外部调用都使用该类
@author:yubang
"""
class Db():
    """
    @param dbInfo 数据库信息字典
    dbInfo={
        'host':数据库主机,
        'port':数据库端口,
        'dbName':数据库,
        'user':数据库用户名,
        'password':数据库密码,
    }
    @param dbType 数据库类型
    @param showSql 是否打印sql
    @param cacheObj 缓存类，建议开发环境提供自己的缓存类，测试环境可以填None
    @param cachePrefix cache key前缀
    @param cacheTimeout 缓存时间
    """
    def __init__(self,dbInfo,dbType="mysql",showSql=True,cacheObj=None,cachePrefix="lightWeight_",cacheTimeout=3600*24,charset="utf8"):
        self.__con=None
        self.dbInfo=dbInfo
        self.showSql=showSql
        self.cache=Cache()
        self.__cacheTimeout=cacheTimeout
        self.__cachePrefix=cachePrefix
        self.__charset=charset
        if(cacheObj==None):
            if(showSql):
                self.log=Log(1)
            else:
                self.log=Log(0)
        else:
            self.log=cacheObj
        #self.__connectionDb()
    def __del__(self):
        #self.__con.close()
        self.log.log("db connection close!",0)
    def __connectionDb(self):
        "尝试连接数据库，5次重试"
        index=0
        max_try_number=5
        while(index<max_try_number):
            try:
                self.__con=MySQLdb.connect(host=self.dbInfo['host'],port=self.dbInfo['port'],user=self.dbInfo['user'],passwd=self.dbInfo['password'],db=self.dbInfo['dbName'],use_unicode=0, charset=self.__charset)
                self.log.log(u"连接数据库!",0)
                return True
            except Exception:
                self.log.log("db connection fail!",0)
            index=index+1
            time.sleep(0.2)
        return False
    def __getConnectionDb(self):
        "获取连接"
        con=self.__con=MySQLdb.connect(host=self.dbInfo['host'],port=self.dbInfo['port'],user=self.dbInfo['user'],passwd=self.dbInfo['password'],db=self.dbInfo['dbName'],use_unicode=0, charset=self.__charset)
        return con
        
    """
    @param tableName 数据表名
    """
    def M(self,tableName):
        "获取一个数据表封装类"
        return Table(tableName,self.__con,self.showSql,self.cache,self.log,self.__getConnectionDb,self.__cacheTimeout,self.__cachePrefix,self.__charset)
    """
    @param timeout int缓存过期时间
    """
    def setCacheTimeout(self,timeout):
        "设置缓存过期时间"
        self.__cacheTimeout=timeout
        
        
"""
数据表封装类
该类用于操作数据表
"""        
class Table():
    """
    @param tableName 数据表名
    @param con 数据库连接对象
    @param showSql 是否打印sql
    @param cache cache类
    @param log 日记类
    @param connectionDb 从新连接数据库的函数
    @param cacheTimeout 数据缓存时间
    @param cachePrefix 缓存key前缀
    """
    def __init__(self,tableName,con,showSql,cache,log,connectionDb,cacheTimeout=3600*24,cachePrefix="lightWeight_",charset="UTF-8"):
        self.__tableName=tableName+" "
        self.__con=con
        self.__cache=cache
        self.__log=log
        self.__sql=""
        self.__errorMessage=""
        self.__dbSql=MysqlSql()
        self.__connectionDb=connectionDb
        self.__cacheTimeout=cacheTimeout
        self.__cachePrefix=cachePrefix
        self.__charset=charset
        self.__again()
    def __del__(self):
        if(self.__cursor!=None):
            self.__cursor.close()
            self.__log.log(u"释放游标！",0)
            if(self.__rollbackSign):
                self.rollback()
        self.__log.log("table class destroy!",0)
    def __again(self):
        "删除作废信息"
        self.__whereText=""
        self.__fieldText=""
        self.__columns=[]
        self.__rollbackSign=False
        self.__cursor=None
        self.__limitText=""
        self.__groupByText=""
        self.__orderByText=""
    def __flush(self):
        "清空缓存，防止修改数据后不一致"
        cacheKey=self.__cachePrefix+"tableCacheKey_"+self.__tableName
        cacheContent=self.__cache.get(cacheKey)
        if(cacheContent!=None):
            cacheContent=json.loads(cacheContent)
            for key in cacheContent:
                self.__log.log(u'删除缓存：'+key,0)
                self.__cache.delete(key)
            self.__cache.delete(cacheKey)
    def __loginCacheKey(self,key):
        "记录该数据表用的cache key，以便于清除"
        cacheKey=self.__cachePrefix+"tableCacheKey_"+self.__tableName
        cacheContent=self.__cache.get(cacheKey)
        if(cacheContent==None):
            cacheContent=[]
        else:
            cacheContent=json.loads(cacheContent)
        cacheContent.append(key)
        #+100是为了保证该缓存比别的缓存晚失活
        self.__cache.set(cacheKey,json.dumps(cacheContent),self.__cacheTimeout+100)
    def __showSql(self):
        "打印执行的sql"
        text=u"执行的sql："+self.__sql
        self.__log.log(text,1)
    def __getAllColumns(self):
        "获取table的所有字段"
        #尝试获取缓存
        cacheKey=self.__cachePrefix+"columns_"+self.__tableName
        tempCache=self.__cache.get(cacheKey)
        if(tempCache!=None):
            self.__log.log(u"从缓存获取到字段",0)
            self.__columns=json.loads(tempCache)
            return True
        
        tempSql=self.__sql
        self.__sql=self.__dbSql.get_ShowTableColumnsSql(self.__tableName)
        self.__columns=self.__buildData(4,False)
        
        self.__sql=tempSql
        if(self.__columns==None):
            return False
        else:
            #写入缓存
            self.__cache.set(cacheKey,json.dumps(self.__columns),self.__cacheTimeout)
            return True
    def __dealField(self,data,sign):
        "处理字段，防止sql注入"
        if(data==None):
            return 'null'
        if(type(data)!=str and type(data).__name__!="unicode"):
            data=str(data)
        data=re.sub(r'\'','\\\'',data)
        if(sign and data != None):
            data="'"+data+"'"
        return data
    def __createSelectSql(self):
        "创建查询语句"
        if(self.__fieldText==""):
            index=0
            self.__getAllColumns()
            if(self.__columns==None):
                return False
            self.__columns.sort()#字段排序
            while(index<len(self.__columns)):
                if(index!=0):
                    self.__fieldText=self.__fieldText+","
                self.__fieldText=self.__fieldText+self.__columns[index]
                index=index+1
        self.__sql="SELECT %s FROM %s"%(self.__fieldText,self.__tableName)
    def __createAnalySql(self,text,analyType):
        "构建统计sql"
        if(text!=None):
            texts=text.split(",")
            self.__fieldText=""
            index=0
            while(index<len(texts)):
                if(index!=0):
                    self.__fieldText=self.__fieldText+","
                self.__fieldText=self.__fieldText+"%s(%s)"%(analyType,texts[index])
                index=index+1
            type=2
        else:
            self.__fieldText="%s(*)"%(analyType)
            type=7
        self.__sql="SELECT %s FROM %s"%(self.__fieldText,self.__tableName)
        return type
    def __createInsertSql(self,data):
        "构建插入语句"
        keys=data.keys()
        index=0
        fieldText=""
        valueText=""
        for key in keys:
            self.__columns.append(key)
            if(index!=0):
                fieldText=fieldText+','
                valueText=valueText+','
            fieldText=fieldText+self.__dealField(key,False)
            valueText=valueText+self.__dealField(data[key],True)
            index=index+1
        self.__sql="INSERT INTO %s(%s) VALUES(%s);"%(self.__tableName,fieldText,valueText)
    
    def __createUpdateSql(self,data):
        "构建插入语句"
        keys=data.keys()
        index=0
        text=""
        for key in keys:
            self.__columns.append(key)
            if(index!=0):
                text=text+" AND "
            text=text+self.__dealField(key,False)+"="+self.__dealField(data[key],True)
            index=index+1
        self.__sql="UPDATE %s SET %s "%(self.__tableName,text)
    def __createDeleteSql(self):
        "构建delete语句"
        self.__sql="DELETE from %s "%(self.__tableName)
    def __createWhereSql(self):
        "构建sql语句"
        if self.__whereText=="":
            self.__whereText=" WHERE 1"
        self.__sql=self.__sql+self.__whereText
        self.__sql=self.__sql+" "+self.__groupByText
        self.__sql=self.__sql+" "+self.__orderByText
        self.__sql=self.__sql+" "+self.__limitText
        self.__sql=self.__sql+";"
    def __getCursor(self):
        "获取游标"
        if(self.__cursor==None):
            self.__cursor=self.__con.cursor()
            self.__cursor.execute(self.__dbSql.get_CloseAutocommit());
            self.__log.log(u"获取游标！",0)
        return self.__cursor
    """
    封装返回数据
    @param sql sql语句
    @param type 返回类型(0 list 1 str 2 int数组 3 dict 4获取字段专用标志 5返回影响行数 6原样返回 7 int)
    @param commitSign 是否需要提交事务
    """
    def __buildData(self,type,commitSign):
        
        '''
        #防止数据库连接断开
        try:
            self.__con.ping()
        except Exception,e:
            self.__log.log(u"发现数据库连接断开！",0)
            if(self.__connectionDb()==False):
                self.__errorMessage=str(e)
                return None
        '''
        
        #获取连接
        self.__con=self.__connectionDb()
        
        try:
            self.__showSql()
            cur=self.__getCursor()
            nums=cur.execute(self.__sql)
            
            if(type==0):
                results=cur.fetchall()
                result=[]
                for temp in results:
                    obj={}
                    index=0
                    for t in self.__columns:
                        obj[t]=str(temp[index])
                        obj[t]=obj[t].decode(self.__charset)
                        index=index+1
                    result.append(obj)
            elif(type==1):
                pass
            elif(type==2):
                results=cur.fetchone()
                result=[]
                for temp in results:
                    result.append(str(temp))
            elif(type==4):
                results=cur.fetchall()
                result=[]
                for temp in results:
                    result.append(temp[0])
            elif(type==5):
                result=nums
                result=cur.lastrowid
            elif(type==6):
                result=cur.fetchall()
            elif(type==7):
                results=cur.fetchone()
                result=results[0]
            
            
            if(commitSign and not self.__rollbackSign):
                self.__log.log(u'事务提交',0)
                self.__con.commit()
                self.__con.close()
            elif(not self.__rollbackSign):
                self.__cursor=None
                self.__con.close()
                
        except Exception,e:
            self.__errorMessage=str(e)
            result=None
            raise
            return []
            
        return result
    def __changeColumnsToStr(self):
        "字段转换成字符串"
        self.__getAllColumns()
        index=0
        data=""
        for temp in self.__columns:
            if(index!=0):
                data=data+","
            data=data+temp
            index=index+1
        return data
    """
    查询缓存
    @param type 该参数传入__buildData函数
    """
    def __useCacheInSelect(self,type):
        "缓存查询数据"
        #由于缓存在分布式时会产生问题，因此禁用缓存
        
        result=self.__buildData(type,False)
        self.__again()#删除作废信息
        
        return result
    """
    @param text 要查询的字段（str）
    text格式 a,b,c
    """
    def field(self,text):
        "查询字段"
        self.__fieldText=text
        strs=text.split(",")
        for temp in strs:
            self.__columns.append(temp)
        return self
    """
    输入where约束
    @param data where字符串或者字典
    具体用法参见API手册
    """
    def where(self,data=None):
        if(data==None):
            pass
        elif(type(data)==dict):
            keys=data.keys()
            index=0
            while(index<len(keys)):
                if(type(data[keys[index]])==type([])):
                    value=data[keys[index]][1]
                    fuhao=data[keys[index]][0]
                    fuhao=fuhao.upper()
                    if(fuhao=="GT"):
                        fuhao=" > "
                        value=self.__dealField(value,True)
                    elif(fuhao=="GTE"):
                        fuhao=" >= "
                        value=self.__dealField(value,True)
                    elif(fuhao=="LT"):
                        fuhao=" < "
                        value=self.__dealField(value,True)
                    elif(fuhao=="LTE"):
                        fuhao=" <= "
                        value=self.__dealField(value,True)
                    elif(fuhao=="EQ"):
                        fuhao=" == "
                        value=self.__dealField(value,True)
                    elif(fuhao=="NEQ"):
                        fuhao=" != "
                        value=self.__dealField(value,True)
                    elif(fuhao=="LIKE"):
                        fuhao=" LIKE "
                        value=self.__dealField(value,True)
                    elif(fuhao=="BETWEEN"):
                        fuhao=" BETWEEN "
                        value=value[0]+" AND "+self.__dealField(value[1],True)
                    elif(fuhao=="NOT BETWEEN"):
                        fuhao=" NOT BETWEEN "
                        value=value[0]+" AND "+self.__dealField(value[1],True)
                    elif(fuhao=="IN" or fuhao=="NOT IN"):
                        if(fuhao=="IN"):
                            fuhao=" IN "
                        else:
                            fuhao=" NOT IN "
                        v=""
                        i=0
                        for temp in value:
                            if(i!=0):
                                v=v+","
                            v=v+self.__dealField(temp,True)
                            i=i+1
                        v="("+v+")"
                        value=v
                else:
                    value=self.__dealField(data[keys[index]],True)
                    fuhao="="
                if(index!=0):
                    self.__whereText=self.__whereText+" AND "
                else:
                    self.__whereText=" WHERE "
                self.__whereText=self.__whereText+self.__dealField(keys[index],False)+fuhao+value
                index=index+1
        else:
            self.__whereText=data
        return self
    def limit(self,row,offset=None):
        "limit约束"
        if(offset==None):
            self.__limitText=" LIMIT %d"%(int(row))
        else:
            self.__limitText=" LIMIT %d,%d"%(int(row),int(offset))
        return self
    def group_by(self,text):
        "group by 约束"
        self.__groupByText=" GROUP BY "+text
        return self
    """
    @param text order字符串
    如果想构建 order by a desc,b asc     text参数为 -a,b
    """
    def order_by(self,text):
        "order by排序"
        self.__orderByText=" "
        texts=text.split(",")
        index=0
        for temp in texts:
            if(index!=0):
                self.__orderByText=self.__orderByText+","
            if(re.search(r'^[\s]*-',temp)):
                self.__orderByText=self.__orderByText+temp.replace("-","")+" DESC"
            else:
                self.__orderByText=self.__orderByText+temp+" ASC"
            index=index+1
        self.__orderByText=" ORDER BY "+self.__orderByText
        return self
    """
    select查询
    """
    def select(self):
        self.__createSelectSql()
        self.__createWhereSql()
        return self.__useCacheInSelect(0)
    """
    update操作
    """    
    def update(self,data):
        self.__createUpdateSql(data)
        self.__createWhereSql()
        result=self.__buildData(5,True)
        self.__flush()
        return result
    """
    insert操作
    """
    def add(self,data):
        self.__createInsertSql(data)
        result=self.__buildData(5,True)
        self.__flush()
        return result
    """
    delete 操作
    """
    def delete(self):
        "删除操作"
        self.__createDeleteSql()
        self.__createWhereSql()
        result=self.__buildData(5,True)
        self.__flush()
        return result
    def getError(self):
        "获取错误信息"
        return self.__errorMessage
    def query(self,sql):
        "执行原生的sql查询"
        self.__sql=sql
        return self.__buildData(6,True)
    def execute(self,sql):
        "执行原生的sql"
        self.__sql=sql
        return self.__buildData(5,True)
    def begin(self):
        "开启事务"
        self.__rollbackSign=True
        self.__con.begin()
        self.__log.log(u'事务开启',0)
    def commit(self):
        "提交事务"
        self.__rollbackSign=False
        self.__con.commit()
        self.__log.log(u'提交事务',0)
    def rollback(self):
        "事务回滚"
        self.__rollbackSign=False
        self.__con.rollback()
        self.__log.log(u'事务回滚',0)
    def getTableColumns(self):
        "获取table的字段"
        self.__getAllColumns()
        return self.__columns
    """
    @param data 要统计的字符串
    data字符串格式为a,b,c
    """
    def count(self,data=None):
        "count操作"
        type=self.__createAnalySql(data,"COUNT")
        self.__createWhereSql()
        return self.__useCacheInSelect(type)
    """
    @param data 要统计的字符串
    data字符串格式为a,b,c
    """
    def avg(self,data=None):
        "avg操作"
        if(data==None):
            data=self.__changeColumnsToStr()
        type=self.__createAnalySql(data,"AVG")
        self.__createWhereSql()
        return self.__useCacheInSelect(type)
    """
    @param data 要统计的字符串
    data字符串格式为a,b,c
    """
    def max(self,data=None):
        "max操作"
        if(data==None):
            data=self.__changeColumnsToStr()
        type=self.__createAnalySql(data,"MAX")
        self.__createWhereSql()
        return self.__useCacheInSelect(type)
    """
    @param data 要统计的字符串
    data字符串格式为a,b,c
    """
    def min(self,data=None):
        "min操作"
        if(data==None):
            data=self.__changeColumnsToStr()
        type=self.__createAnalySql(data,"MIN")
        self.__createWhereSql()
        return self.__useCacheInSelect(type)
    """
    获取最后执行的sql语句
    """
    def getLastSql(self):
        return self.__sql
        
    
"""
处理mysql
@author:yubang
"""        
class MysqlSql():
    "封装特殊的mysql专用sql"
    def get_ShowTableColumnsSql(self,tableName):
        return "show columns from "+tableName+";"
    def get_CloseAutocommit(self):
        return "set autocommit = 0;"

        
class LightWeightDb():
    "一个数据库线程池类"
    def __init__(self):
        self.__maxDbNumber=5
        self.__dbSelectIndex=0
        self.__debug=True
        self.__dbObjs=[]
        self.__initSign=False
        self.__dbType="mysql"
        self.__showSql=True
        self.__cacheObj=None
        self.__cachePrefix="lightWeight_"
        self.__cacheTimeout=3600*24
        self.__charset="utf8"
    def setMaxDbNumber(self,number):
        "设置数据库连接数"
        self.__maxDbNumber=number
    def setDbConfig(self,dbInfo):
        "设置数据库信息"
        self.__dbInfo=dbInfo
    def __initDb(self):
        "初始化数据库"
        index=0
        while(index<self.__maxDbNumber):
            obj=Db(self.__dbInfo,self.__dbType,self.__debug,self.__cacheObj,self.__cachePrefix,self.__cacheTimeout,self.__charset)
            self.__dbObjs.append(obj)
            index=index+1
    def setDebug(self,sign):
        "是否打开日记"
        self.__debug=sign
    def setCharset(self,charset):
        self.__charset=charset
    """
    获取db对象
    @param dbInfo 一个数据库配置字典
    """        
    def getDb(self):
        "获取一个db对象"
        if(not self.__initSign):
            self.__initDb()
        obj=self.__dbObjs[self.__dbSelectIndex]
        self.__dbSelectIndex=self.__dbSelectIndex+1
        if(self.__dbSelectIndex>=self.__maxDbNumber):
            self.__dbSelectIndex=0
        return obj
        
"""
获取一个数据库线程类，建议使用该方法
"""        
lightWeightDb=None
def getLightWeightDb():
    global lightWeightDb
    if(lightWeightDb==None):
        lightWeightDb=LightWeightDb()
    return lightWeightDb

    
if __name__ == "__main__":
    dbInfo={
        'host':'127.0.0.1',
        'port':3306,
        'dbName':'mysql',
        'user':'root',
        'password':'root',
    }
    #示例1
    lightWeightDbObj=getLightWeightDb()
    lightWeightDbObj.setDbConfig(dbInfo)
    lightWeightDbObj.setMaxDbNumber(1)
    db=lightWeightDbObj.getDb()
    dao=db.M("db")
    print dao.select()
    print dao.getLastSql()
    #time.sleep(20)
    
    #示例2
    #db=Db(dbInfo)
    #dao=db.M("db")
    #print dao.select()
    
