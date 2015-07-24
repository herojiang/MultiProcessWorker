#coding=utf-8
from __future__ import division

from threading import Thread
import os
from time import ctime,sleep
import subprocess
import glob
import re
import shutil
import sys

class worker():
    workspace=''
    argc=0
    args=[]
    func=""
    def __init__(self,w):
        self.workspace=w
        
    def setArgc(self,a):
        self.argc = a
        
    def setArgs(self,a):
        self.args=[]
        for arg in a:
            self.args.append(arg.replace("%WORKERROOT%",self.workspace))
        
    def setFunc(self,f):
        self.func = f
        
    def createWorkerThread(self):
        if(self.argc == 1):
            self.thread=Thread(target=self.func,args=(self.args[0],))
        else:
            self.thread=Thread(target=self.func,args=(self.args[0],self.args[1]))
        self.thread.setDaemon(True)
        return self.thread
    
    def getWorkerThread(self):
        return self.thread

def runProcess(command):
    ret = subprocess.call(command)
    if(ret!=0):
        print "错误：处理命令%s失败！".decode("utf-8").encode("GBK") %command
        sys.exit(-1)
    
class workerManager():
    nWorker=0
    workerList=[]
    def __init__(self,baseWorkspace,n):
        self.nWorker=n
        for i in range(1,n+1):
            workerdir = baseWorkspace+"worker%d"%(i)
            if(not os.path.exists(workerdir)):
                print "错误：worker工作目录%s不存在！".decode("utf-8").encode("GBK") %workerdir
                sys.exit(-1)
            w=worker(workerdir)
            self.workerList.append(w)
            
    def setArgs(self,a):
        for worker in self.workerList:
            worker.setArgs(a)

    def setArgc(self,a):
        for worker in self.workerList:
            worker.setArgc(a)
            
    def setFunc(self,f):
        for worker in self.workerList:
            worker.setFunc(f)
            
    def runInManyThreads(self):
        for worker in self.workerList:  
            worker.createWorkerThread().start()
        
        for worker in self.workerList: 	
            worker.getWorkerThread().join()
    '''        
    [description]
    SystemRun方法的并发版，所有worker同时执行，并等待他们全部结束
    '''               
    def SystemRun(self,cmd,*args):
        for arg in args:
            cmd+=" "+arg
        self.setFunc(runProcess)
        self.setArgc(1)
        cmdArgs=[]
        cmdArgs.append(cmd)        
        self.setArgs(cmdArgs)
        self.runInManyThreads()
    '''        
    [description]
    rarMT方法的并发版，所有worker同时执行，并等待他们全部结束
    '''   
    def rarMT(self,src,des):
        self.setFunc(rarMT)
        self.setArgc(2)
        cmdArgs=[]
        cmdArgs.append(src)
        cmdArgs.append(des)
        self.setArgs(cmdArgs)
        self.runInManyThreads()
    '''        
    [description]
    将所有worker下workerDataDir目录下的数据拷贝到des目录下
    [input]
    workerDataDir：所有worker待拷贝的目录
    des：数据将拷贝到的目的目录
    [return]
    无
    [memo]
    如果worker数量大于待平分rar的数量，有些worker将分不到数据。
    '''         
    def collectFromWorker(self,workerDataDir,des):
        self.setFunc(CopyContentInDir)
        self.setArgc(2)
        cmdArgs=[]
        cmdArgs.append(workerDataDir)
        cmdArgs.append(des)
        self.setArgs(cmdArgs)
        self.runInManyThreads()

    '''        
    [description]
    将srcDir目录下的rar按照worker数据进行平分，分别存放到worker的desDir下
    [input]
    srcDir：待平分rar所在的目录
    desDir：各个worker的目的目录
    [return]
    无
    [memo]
    如果worker数量大于待平分rar的数量，有些worker将分不到数据。
    ''' 
    def SplitRarMesh(self,srcDir,desDir):
        srcDir=srcDir.strip()
        srcDir=srcDir.rstrip("\\")
        desDir=desDir.strip()
        desDir=desDir.rstrip("\\")
        files = glob.glob(srcDir + '\\*.rar')
        nfiles = len(files)
        if(nfiles==0):
            print "错误：目录%s无需要划分的rar文件！".decode("utf-8").encode("GBK") %srcDir  
            sys.exit(-1)

        nGroup = int(round(nfiles/self.nWorker,0))
        if(nGroup < 1):
            nGroup = 1
        
        i,j = 0,0
        for file in files:
            if(i>=nGroup):
                i,j = 0,j+1
            relDesDir=desDir.replace("%WORKERROOT%",self.workerList[j].workspace)
            CopyFile(file,relDesDir)
            i=i+1
    '''        
    [description]
    将srcDir目录下的文件夹按照worker数据进行平分，分别存放到worker的desDir下
    [input]
    srcDir：待平分文件夹的目录
    desDir：各个worker的目的目录
    [return]
    无
    [memo]
    如果worker数量大于待平分文件夹数量，有些worker将分不到数据。
    ''' 
    def SplitDirMesh(self,srcDir,desDir):
        srcDir=srcDir.strip()
        srcDir=srcDir.rstrip("\\")
        desDir=desDir.strip()
        desDir=desDir.rstrip("\\")
        
        dirs = os.listdir(srcDir)
        for d in dirs:
            if(d.find(".")>0):
                dirs.remove(d)

        ndirs = len(dirs)
        if(ndirs==0):
            print "错误：目录%s无需要划分的mesh目录！".decode("utf-8").encode("GBK") %srcDir  
            sys.exit(-1)
        
        nGroup = int(round(ndirs/self.nWorker,0))
        if(nGroup < 1):
            nGroup = 1
        i,j = 0,0
        for dir in dirs:
            if(i>=nGroup):
                i,j = 0,j+1
            relDesDir=desDir.replace("%WORKERROOT%",self.workerList[j].workspace)
            CopyDir(srcDir+"\\"+dir,relDesDir+"\\"+dir)
            i=i+1
    '''        
    [description]
    清空所有worker的指定目录
    [input]
    path：所有worker的制定目录
    [return]
    无
    [memo]
    '''  
    def clearDir(self,path):
        for w in self.workerList:
            relDesDir=path.replace("%WORKERROOT%",w.workspace)
            ClearDir(relDesDir)
'''        
[description]
创建文件夹
[input]
path：待创建的文件夹全路径
[return]
无
[memo]
'''             
def mkdir(path):
    path=path.strip()
    path=path.rstrip("\\")
    try:
        isExists=os.path.exists(path)
        if not isExists:
            os.makedirs(path)
    except Exception:
        print "错误：创建目录%s失败！".decode("utf-8").encode("GBK") %path
        sys.exit(-1)
'''        
[description]
压缩源文件到目的路径
[input]
src：待压缩文件的全路径
des：压缩后的rar文件路径（包括rar文件名）
[return]
无
[memo]
'''         		
def rarFile(src,des):
    subprocess.call('Rar.exe a %s %s -ep1' %(des,src))
'''        
[description]
解压缩源rar文件到目的路径
[input]
srcRar：待解压的rar文件全路径
desDir：目的目录，为空表示解压到源目录
[return]
无
[memo]
''' 
def unrar(srcRar,desDir):
    if(desDir!=""):
        subprocess.call('Rar.exe x %s %s' %(srcRar,desDir))
    else:
        subprocess.call('Rar.exe x %s' %(srcRar))
'''        
[description]
压缩源目录为目的目录的文件夹
[input]
srcDir：待压缩的源目录
desRar：压缩后的rar文件全路径（包括rar文件名）
[return]
无
[memo]
'''  
def rarDir(srcDir,desRar):
    subprocess.call('Rar.exe a %s %s -ep1 -r' %(desRar,srcDir))
'''        
[description]
压缩源目录下的所有一级子文件夹到目的目录下，形成若干rar文件
[input]
src：存放子文件夹的源目录
des：存放rar文件的目的目录
[return]
无
[memo]
'''  
def rarMT(src,des):
    mkdir(des)
    dirs = os.listdir(src)
    for d in dirs:
        if(d.find(".")>0):
            dirs.remove(d)

    for dir in dirs:            
        rarDir(src+"\\"+dir,des+"\\"+dir)

'''        
[description]
解压源目录下的所有rar文件到目的目录下
[input]
src：存放rar文件的源目录
des：存放解压文件的目的目录
[return]
无
[memo]
'''     
def unrarMT(src,des):
    mkdir(des)
    files = glob.glob(src + '\\*.rar')
    if(len(files)==0):
        print "错误：目录%s无需要解压的rar文件！".decode("utf-8").encode("GBK") %src
        sys.exit(-1)
        
    for file in files :
        unrar(file,des)
'''        
[description]
获得CPU的物理核心数
[input]
无
[return]
物理核心数
[memo]
''' 
def GetCPUPhysicalCores():
    p = subprocess.Popen("showGetLogicalProcessorInformation.exe",stdin = subprocess.PIPE,stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell = False)
    res = re.compile(r'\d+')
    nCores = res.findall(p.stdout.read())
    return nCores[0]
'''        
[description]
获得CPU的逻辑核心数
[input]
无
[return]
逻辑核心数
[memo]
'''        
def GetCPUlogicalCores():
    p = subprocess.Popen("showGetLogicalProcessorInformation.exe",stdin = subprocess.PIPE,stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell = False)
    res = re.compile(r'\d+')
    nCores = res.findall(p.stdout.read())
    return nCores[1]
'''        
[description]
创建子进程运行外部程序，并等待子进程结束
[input]
cmd：外部程序的命令
args：外部程序的参数列表
[return]
无
[memo]
根据外部程序的返回值判断执行结果，为0则成功，不为0则处理失败，整个流程也就失败了，必须退出
'''
def SystemRun(cmd,*args):
    for arg in args:
        cmd+=" "+arg
    ret = subprocess.call(cmd)
    if(ret!=0):
        print "错误：处理命令%s失败！".decode("utf-8").encode("GBK") %cmd
        sys.exit(-1)
'''        
[description]
将源文件拷贝到目的目录或目的文件，若des为目录，则源文件被拷贝到该目录下。如des为文件，则拷贝为该文件。若已存在该文件则失败
[input]
src：源文件的完整路径
des：目的目录或目的文件路径
[return]
无
[memo]
失败则退出程序，因为可能引起整个流程处理错误
'''
def CopyFile(src,des): 
    try:
        shutil.copy(src,des)
    except Exception:
        print "错误：拷贝文件到%s失败！".decode("utf-8").encode("GBK") %des
        sys.exit(-1)
'''        
[description]
将源目录拷贝为目的目录，文件名称变更为des目录，如已存在同名目录，则拷贝失败
[input]
src：源目录完整路径
des：目的目录完整路径
[return]
无
[memo]
失败则退出程序，因为可能引起整个流程处理错误
'''     
def CopyDir(src,des):
    try:
        shutil.copytree(src,des)
    except Exception:
        print "错误：拷贝目录到%s失败！".decode("utf-8").encode("GBK") %des
        sys.exit(-1)
'''        
[description]
拷贝源目录下的所有文件到目录目录，文件名称不变
[input]
src：源目录完整路径
des：目的目录完整路径
[return]
无
''' 
def CopyContentInDir(src,des):
    filelist = os.listdir(src)
    for f in filelist:
        srcPath = os.path.join(src, f)
        desPath = os.path.join(des, f)
        if os.path.isfile(srcPath):
            CopyFile(srcPath,desPath)
        else:
            CopyDir(srcPath,desPath)
'''        
[description]
删除整个目录，该根目录不保留
[input]
path：根目录完整路径
[return]
无
[memo]
失败则退出程序，因为可能引起整个流程处理错误
'''              
def RemoveDir(path):
    try:
        shutil.rmtree(path)
    except Exception:
        print "错误：删除目录%s失败！".decode("utf-8").encode("GBK") %path
        sys.exit(-1)
'''        
[description]
清空该目录下的所有文件和文件夹，该目录保留
[input]
rootdir：根目录完整路径
[return]
无
'''        
def ClearDir(rootdir):
    filelist = os.listdir(rootdir)
    for f in filelist:
        filepath = os.path.join(rootdir, f)
        if os.path.isfile(filepath):  
            os.remove(filepath)
        else:
            RemoveDir(filepath)

    

    




