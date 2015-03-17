#coding:UTF-8

from flask import Flask,render_template,redirect,request,session
from lightWeightORM import Db
import time

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
    lists=dao.where().order_by("id").select()
    return render_template("admin.html",lists=lists)

@app.route("/addMessage",methods=['POST'])
def addMessage():
    "添加信息"
    global db
    title=request.form.get("title",None)
    status=request.form.get('status',None)
    
    dao=db.M("cms_message")
    dao.add({"title":title,"status":status})
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
    return render_template("message.html")
    
if __name__ == "__main__":
    app.run(debug=True,port=8000,host="127.0.0.1")
