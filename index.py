#coding:UTF-8

from flask import Flask,render_template,redirect,request,session,abort
from lightWeightORM import Db
import time,hashlib

app=Flask(__name__)
app.secret_key="root"

dbInfo={
        'host':'127.0.0.1',
        'port':3306,
        'dbName':'cms',
        'user':'root',
        'password':'root',
    }
db=Db(dbInfo)

@app.route("/")
def index():
    "主页"
    
    if(session.has_key("uid")):
        #已登录用户直接进入后台
        return redirect("/admin")
    
    if(request.args.get("error",None)==None):
        error=False
    else:
        error=True
    return render_template("index.html",error=error)

@app.route("/login",methods=['POST'])
def login():
    "登录"
    username=request.form.get("username",None)
    password=request.form.get("password",None)
    if(username=="root" and password=="root"):
        session['uid']=time.time()
        return redirect("/admin")
    else:
        return redirect("/?error=1")

@app.route("/admin")    
def admin():
    "后台面板"
    global db
    dao=db.M("cms_message")
    lists=dao.where().order_by("-id").select()
    return render_template("admin.html",lists=lists)

@app.route("/addMessage",methods=['POST'])
def addMessage():
    "添加信息"
    global db
    title=request.form.get("title",None)
    status=request.form.get('status',None)
    token=hashlib.md5(str(time.time())).hexdigest()
    
    dao=db.M("cms_message")
    id=dao.add({"title":title,"status":status,"token":token})
    dao.where({"id":id}).update({"code":hashlib.md5(str(id)).hexdigest()})
    return redirect("/admin")

@app.route("/deleteMessage")
def deleteMessage():
    "删除信息"
    global db
    
    id=request.args.get("id",None)
    
    dao=db.M("cms_message")
    dao.where({"id":id}).delete()
    return redirect("/admin")


@app.route("/message")
def message():
    "信息展示"
    global db
    
    token=request.args.get("token",None)
    code=request.args.get("code",None)
    
    dao=db.M("cms_message")
    objs=dao.where({"token":token,'code':code}).select()
    if(len(objs)==1):
        obj=objs[0]
    else:
        return abort(404)
    
    dao=db.M("cms_message_content")
    lists=dao.where({"mid":obj['id']}).select()
    
    return render_template("message.html",obj=obj,lists=lists)


@app.route("/editMessage")
def editMessage():
    "编辑信息"
    global db
    
    id=request.args.get("id",None)
    
    dao=db.M("cms_message_content")
    lists=dao.where({"mid":id}).order_by("id").select()
    
    return render_template("editMessage.html",lists=lists,id=id)

@app.route("/addMessageContent",methods=['POST'])
def addMessageContent():
    "添加信息子页面"
    mid=request.form.get("id",None)
    content=request.form.get("content",None)
    
    global db
    dao=db.M("cms_message_content")
    dao.add({'mid':mid,'content':content})
    
    return redirect("/editMessage?id="+mid)

@app.route("/deleteMessageContent")
def deleteMessageContent():
    "删除子页面"
    id=request.args.get("id",None)
    mid=request.args.get("mid",None)
    
    global db
    dao=db.M("cms_message_content")
    dao.where({"id":id,"mid":mid}).delete()
    
    return redirect("/editMessage?id="+mid)
    

@app.route("/editMessageContent",methods=['GET','POST'])
def editMessageContent():
    "子页面编辑"
    global db
    dao=db.M("cms_message_content")
    
    id=request.args.get("id",None)
    mid=request.args.get("mid",None)
    
    if(request.method=="GET"):
        objs=dao.where({"id":id,"mid":mid}).select()
        if(len(objs)!=1):
            return abort(404)
        return render_template("editMessageContent.html",obj=objs[0])
    else:
        content=request.form.get("content",None)
        dao.where({"id":id,"mid":mid}).update({"content":content})
        return redirect("/editMessage?id="+mid)
    
@app.route("/exit")
def exit():
    session.pop("uid")
    return redirect("/")
    
if __name__ == "__main__":
    app.run(debug=True,port=8000,host="172.16.45.76")
