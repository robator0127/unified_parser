# sudo nohup python3 -u api_parser.py >> log.txt 2>&1 &
import argparse,pdb
import json
import os
os.environ['CUDA_VISIBLE_DEVICES'] = '0' #设置工作的显卡位置
import shutil
from typing import List, Optional
import pydantic
import uvicorn
from fastapi import Body, FastAPI, File, Form, Query, UploadFile, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing_extensions import Annotated
from starlette.responses import RedirectResponse
from langchain.document_loaders import UnstructuredFileLoader, TextLoader


from utils.api_utils import BaseResponse, SummaryResponse
import datetime
from time import ctime
from loguru import logger

from loader import UnstructuredPaddleImageLoader, UnstructuredPaddlePDFLoader,UnstructuredWordLoader,UnstructuredAudioLoader
from langchain.text_splitter import CharacterTextSplitter
from io import BytesIO
import requests
from OpenSSL import crypto, SSL

OPEN_CROSS_DOMAIN=False
ROBOT_FILE_SUMMARY = "./sourcefiles"

# 文本分句长度
SENTENCE_SIZE = 100

# 匹配后单段上下文长度
CHUNK_SIZE = 250

def generate_certificate(
		organization="Haier",
		common_name="113.105.90.161:8020",
		country="NL",
		duration=(365 * 24 * 60 * 60),
		keyfilename="key.pem",
		certfilename="cert.pem"):
	k = crypto.PKey()
	k.generate_key(crypto.TYPE_RSA, 4096)

	cert = crypto.X509()
	cert.get_subject().C = country
	cert.get_subject().O = organization
	cert.get_subject().CN = common_name
	cert.gmtime_adj_notBefore(0)
	cert.gmtime_adj_notAfter(duration)
	cert.set_issuer(cert.get_subject())
	cert.set_pubkey(k)
	cert.sign(k, 'sha512')
	with open(keyfilename, "wt") as keyfile:
		keyfile.write(crypto.dump_privatekey(crypto.FILETYPE_PEM, k).decode("utf-8"))
	with open(certfilename, "wt") as certfile:
		certfile.write(crypto.dump_certificate(crypto.FILETYPE_PEM, cert).decode("utf-8"))    

class ChineseTextSplitter(CharacterTextSplitter):
    def __init__(self, pdf: bool = False, sentence_size: int = SENTENCE_SIZE, **kwargs):
        super().__init__(**kwargs)
        self.pdf = pdf
        self.sentence_size = sentence_size

    def split_text(self, text: str) -> List[str]:
        return [text]
def load_file(filepath, sentence_size=SENTENCE_SIZE,file_id="",user_id="",page_num=1,pages=False,enhanced=True):
    textsplitter = ChineseTextSplitter(pdf=False, sentence_size=sentence_size)    
    if filepath.lower().endswith(".md"):
        loader = UnstructuredFileLoader(filepath)
        docs = loader.load()
    elif filepath.lower().endswith(".txt"):
        loader = TextLoader(filepath, autodetect_encoding=True)
        #loader = TextLoader(filepath, encoding="utf-8", autodetect_encoding=True)
        docs = loader.load_and_split(textsplitter)
        
    elif filepath.lower().endswith(".pdf"):
        loader = UnstructuredPaddlePDFLoader(filepath,mode="elements",pages=pages,enhanced=enhanced)
#         textsplitter = ChineseTextSplitter(pdf=True, sentence_size=sentence_size)
        docs = loader.load_and_split(textsplitter)
        
    elif filepath.lower().endswith(".jpg") or filepath.lower().endswith(".png") or filepath.lower().endswith(".jpeg"):
        loader = UnstructuredPaddleImageLoader(filepath, mode="elements")
#         textsplitter = ChineseTextSplitter(pdf=False, sentence_size=sentence_size)
        docs = loader.load_and_split(text_splitter=textsplitter)
    elif filepath.lower().endswith(".mp3") or filepath.lower().endswith(".wav") :
        loader = UnstructuredAudioLoader(filepath, mode="elements")
#         textsplitter = ChineseTextSplitter(pdf=False, sentence_size=sentence_size)
        docs = loader.load_and_split(text_splitter=textsplitter)      
    elif filepath.lower().endswith(".docx") or filepath.lower().endswith(".doc"):
        loader = UnstructuredWordLoader(filepath,mode="elements",pages=pages)
        docs = loader.load_and_split(text_splitter=textsplitter)
    else:
        loader = UnstructuredFileLoader(filepath, mode="elements")
#         textsplitter = ChineseTextSplitter(pdf=False, sentence_size=sentence_size)
        docs = loader.load_and_split(text_splitter=textsplitter)
    return docs

async def unified_parser_columns( #async 
        file: UploadFile = File(description="A single binary file"),
        file_id: str = Body(..., description="File ID"),
        user_id: str = Body(..., description="User ID"),
        file_name:str = Body(..., description="file  name"),
        
):
    """
    统一解析器，解析成字符串,文档类支持pdf,word,图片类支持jpg、jpeg或png格式，音频类支持mp3、wav等格式，本接口对pdf 分栏效果要好些
    """
    logger.info('file_id:{0},user_id:{1},file_name:{2},file.filename:{3},time is {4}'.format(file_id,user_id,file_name,file.filename,ctime()))
    if not file_name: #为空
        file_name=file.filename
    path_temp=(file_id+'_'+user_id+"_"+file_name).replace("/","").replace("\\","")
    file_path = os.path.join(ROBOT_FILE_SUMMARY, path_temp)
    if not os.path.exists(file_path): #当前目录不存在
        try:
            file_content =  await file.read()  # 读取上传文件的内容，await 
            if file_content == "":
                logger.error("上传文件为空:{}", e)
                return SummaryResponse(code=500, msg="上传文件失败", data="")
            with open(file_path, "wb") as f:
                f.write(file_content)
        except Exception as e:
            logger.error("上传文件失败:{}", e)
            return SummaryResponse(code=500, msg="上传文件失败", data="")  
    if not os.path.exists(file_path):
        logger.error("文件保存失败:{}", e)
        return SummaryResponse(code=500, msg="上传文件失败", data="")    
    #判断中间文件是否存在
    if file_name.endswith('.txt'): #txt 没有中间文件
        file_path_text = file_path
    else:
        file_path_text = os.path.join(os.path.join(ROBOT_FILE_SUMMARY,'tmp_files'), file_id+'_'+user_id+"_"+file_name+'.txt')      
    if os.path.exists(file_path_text) :
        pass
    else:#不存在中间文件
        print('in unified_parser,load_file again')
        # 解析文件,生成中间文件
        time1 = datetime.datetime.now()
        try:            
            docs = load_file(file_path, sentence_size=SENTENCE_SIZE,file_id=file_id, user_id=user_id,enhanced=True)
            logger.info("{} 已成功加载", file)       
        except Exception as e:
            logger.error("解析文件失败:{}", e)
            return SummaryResponse(code=500, msg="解析文件失败", data="") 
        time2 = datetime.datetime.now()
        logger.info("解析+切分耗时：{}秒", (time2 - time1).seconds)       
    #判断中间文件是否存在
    
    if os.path.exists(file_path_text) :
        with open(file_path_text,'r',encoding='utf-8') as f:
            data=f.read()
            return SummaryResponse(code=200, msg="请求成功", data=data)            
    else:#不存在中间文件
        logger.error("中间文件不存在：{0}".format(file_path_text))
        return SummaryResponse(code=501, msg="解析文件失败",data="")

async def unified_parser( #async 
        file: UploadFile = File(description="A single binary file"),
        file_id: str = Body(..., description="File ID"),
        user_id: str = Body(..., description="User ID"),
        file_name:str = Body(..., description="file  name"),
        
):
    """
    统一解析器，解析成字符串,文档类支持pdf,word，图片类支持jpg、jpeg或png格式，音频类支持mp3、wav等格式
    """
    #pdb.set_trace()
    logger.info('file_id:{0},user_id:{1},file_name:{2},file.filename:{3},time is {4}'.format(file_id,user_id,file_name,file.filename,ctime()))
    if not file_name: #为空
        file_name=file.filename
    path_temp=(file_id+'_'+user_id+"_"+file_name).replace("/","").replace("\\","")
    file_path = os.path.join(ROBOT_FILE_SUMMARY, path_temp)
    if not os.path.exists(file_path): #当前目录不存在
        try:
            file_content =  await file.read()  # 读取上传文件的内容，await 
            if file_content == "":
                logger.error("上传文件为空:{}", e)
                return SummaryResponse(code=500, msg="上传文件失败", data="")
            with open(file_path, "wb") as f:
                f.write(file_content)
        except Exception as e:
            logger.error("上传文件失败:{}", e)
            return SummaryResponse(code=500, msg="上传文件失败", data="")  
    if not os.path.exists(file_path):
        logger.error("文件保存失败:{}", e)
        return SummaryResponse(code=500, msg="上传文件失败", data="")    
    #判断中间文件是否存在
    if file_name.endswith('.txt'): #txt 没有中间文件
        file_path_text = file_path
    else:
        file_path_text = os.path.join(os.path.join(ROBOT_FILE_SUMMARY,'tmp_files'), file_id+'_'+user_id+"_"+file_name+'.txt')
    if os.path.exists(file_path_text) :
        pass
    else:#不存在中间文件
        print('in unified_parser,load_file again')
        # 解析文件,生成中间文件
        time1 = datetime.datetime.now()
        try:            
            docs = load_file(file_path, sentence_size=SENTENCE_SIZE,file_id=file_id, user_id=user_id)
            logger.info("{} 已成功加载", file)       
        except Exception as e:
            logger.error("解析文件失败:{}", e)
            return SummaryResponse(code=500, msg="解析文件失败", data="") 
        time2 = datetime.datetime.now()
        logger.info("解析+切分耗时：{}秒", (time2 - time1).seconds)       
    #判断中间文件是否存在
    
    if os.path.exists(file_path_text) :
        with open(file_path_text,'r',encoding='utf-8') as f:
            data=f.read()
            return SummaryResponse(code=200, msg="请求成功", data=data)           
    else:#不存在中间文件
        logger.error("中间文件不存在：{0}".format(file_path_text))
        return SummaryResponse(code=501, msg="解析文件失败",data="")
async def unified_parser_pages( #async 
        file: UploadFile = File(description="A single binary file"),
        file_id: str = Body(..., description="File ID"),
        user_id: str = Body(..., description="User ID"),
        file_name:str = Body(..., description="file  name"),
):
    """
    统一解析器，解析成字符串,文档类支持pdf,word，图片类支持jpg、jpeg或png格式，音频类支持mp3、wav等格式，本接口针对pdf解析增加了带页码的功能，返回的字符串是一个json数据，里面包含页码信息，
    """
    #pdb.set_trace()
    logger.info('file_id:{0},user_id:{1},file_name:{2},file.filename:{3},time is {4}'.format(file_id,user_id,file_name,file.filename,ctime()))
    if not file_name: #为空
        file_name=file.filename
    path_temp=(file_id+'_'+user_id+"_"+file_name).replace("/","").replace("\\","")
    file_path = os.path.join(ROBOT_FILE_SUMMARY, path_temp)
    if not os.path.exists(file_path): #当前目录不存在
        try:
            file_content =  await file.read()  # 读取上传文件的内容，await 
            if file_content == "":
                logger.error("上传文件为空:{}", e)
                return SummaryResponse(code=500, msg="上传文件失败", data="")
            with open(file_path, "wb") as f:
                f.write(file_content)
        except Exception as e:
            logger.error("上传文件失败:{}", e)
            return SummaryResponse(code=500, msg="上传文件失败", data="")  
    if not os.path.exists(file_path):
        logger.error("文件保存失败:{}", e)
        return SummaryResponse(code=500, msg="上传文件失败", data="")    
    #判断中间文件是否存在
    if file_name.endswith('.txt'): #txt 没有中间文件
        file_path_text = file_path
    else:
        file_path_text = os.path.join(os.path.join(ROBOT_FILE_SUMMARY,'tmp_files'), file_id+'_'+user_id+"_"+file_name+'.txt')
      
    if os.path.exists(file_path_text):
        pass
    else:#不存在中间文件
        print('in unified_parser,load_file again')
        # 解析文件,生成中间文件
        time1 = datetime.datetime.now()
        try:            
            docs = load_file(file_path, sentence_size=SENTENCE_SIZE,file_id=file_id, user_id=user_id,pages=True)
            logger.info("{} 已成功加载", file)       
        except Exception as e:
            logger.error("解析文件失败:{}", e)
            return SummaryResponse(code=500, msg="解析文件失败", data="") 
        time2 = datetime.datetime.now()
        logger.info("解析+切分耗时：{}秒", (time2 - time1).seconds)       
    #判断中间文件是否存在
    
    if os.path.exists(file_path_text):
        with open(file_path_text,'r',encoding='utf-8') as f:
            data=f.read()
            return SummaryResponse(code=200, msg="请求成功", data=data)           
    else:#不存在中间文件
        logger.error("中间文件不存在：{0}".format(file_path_text))
        return SummaryResponse(code=501, msg="解析文件失败",data="")
        
def unified_parser_url( #async 
        url: str = Body(..., description="File ID"),
        file_id: str = Body(..., description="File ID"),
        user_id: str = Body(..., description="User ID"),
        file_name:str = Body(..., description="file  name"),
):
    """
    统一解析器，解析成字符串,文档类支持pdf,word，图片类支持jpg、jpeg或png格式，音频类支持mp3、wav等格式，本接口针对pdf解析增加了带页码的功能，返回的字符串是一个json数据，里面包含页码信息，源文件是url 形式
    """
    #pdb.set_trace()
    logger.info('url:{0},file_id:{1},user_id:{2},file_name:{3},time is {4}'.format(url,file_id,user_id,file_name,ctime()))
    try:
        response = requests.get(url,timeout=120)
    except Exception as e:
        logger.error("url error:{0}".format(e))
        return SummaryResponse( code=505,msg="解析URL出错",data="")        
        
    if response.status_code!=200:
        logger.error("url error,result status_code!=200,result:{0}".format(response))
        return SummaryResponse(code=506,msg="解析URL失败",data="")
                
    
    path_temp=(file_id+'_'+user_id+"_"+file_name).replace("/","").replace("\\","")
    file_path = os.path.join(ROBOT_FILE_SUMMARY, path_temp)
    if not os.path.exists(file_path): #当前目录不存在
        try:
            file_content =  response.content  # 读取上传文件的内容，await 
            if file_content == "":
                logger.error("上传文件为空:{}", e)
                return SummaryResponse(code=500, msg="上传文件失败", data="")
            with open(file_path, "wb") as f:
                f.write(file_content)
        except Exception as e:
            logger.error("上传文件失败:{}", e)
            return SummaryResponse(code=500, msg="上传文件失败", data="")  
    if not os.path.exists(file_path):
        logger.error("文件保存失败:{}", e)
        return SummaryResponse(code=500, msg="上传文件失败", data="")    
    #判断中间文件是否存在
    if file_name.endswith('.txt'): #txt 没有中间文件
        file_path_text = file_path
    else:
        file_path_text = os.path.join(os.path.join(ROBOT_FILE_SUMMARY,'tmp_files'), file_id+'_'+user_id+"_"+file_name+'.txt')
      
    if os.path.exists(file_path_text):
        pass
    else:#不存在中间文件
        print('in unified_parser,load_file again')
        # 解析文件,生成中间文件
        time1 = datetime.datetime.now()
        try:            
            docs = load_file(file_path, sentence_size=SENTENCE_SIZE,file_id=file_id, user_id=user_id,pages=True)
            logger.info("{} 已成功加载", url)       
        except Exception as e:
            logger.error("解析文件失败:{}", e)
            return SummaryResponse(code=500, msg="解析文件失败", data="") 
        time2 = datetime.datetime.now()
        logger.info("解析+切分耗时：{}秒", (time2 - time1).seconds)       
    #判断中间文件是否存在
    
    if os.path.exists(file_path_text):
        with open(file_path_text,'r',encoding='utf-8') as f:
            data=f.read()
            return SummaryResponse(code=200, msg="请求成功", data=data)           
    else:#不存在中间文件
        logger.error("中间文件不存在：{0}".format(file_path_text))
        return SummaryResponse(code=501, msg="解析文件失败",data="")

async def document():
    return RedirectResponse(url="/docs")


def api_start(host, port,ssl_keyfile, ssl_certfile):
    global app

    app = FastAPI()
    # Add CORS middleware to allow all origins
    # 在config.py中设置OPEN_DOMAIN=True，允许跨域
    # set OPEN_DOMAIN=True in config.py to allow cross-domain
    if OPEN_CROSS_DOMAIN:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
    app.get("/", 
            response_model=BaseResponse, 
            summary="swagger 文档")(document)
    #行研机器人解析pdf接口
    app.post("/local_doc_qa/unified_parser_columns", 
             response_model=SummaryResponse, 
             tags=["统一解析器"], 
             summary="文档解析")(unified_parser_columns)    
    app.post("/local_doc_qa/unified_parser", 
             response_model=SummaryResponse, 
             tags=["统一解析器"], 
             summary="文档解析")(unified_parser)
#     带页码
    app.post("/local_doc_qa/unified_parser_pages", 
             response_model=SummaryResponse, 
             tags=["统一解析器"], 
             summary="文档解析")(unified_parser_pages)
    app.post("/local_doc_qa/unified_parser_url", 
             response_model=SummaryResponse, 
             tags=["统一解析器"], 
             summary="文档解析")(unified_parser_url)    
    uvicorn.run(app, host=host, port=port,ssl_keyfile=ssl_keyfile, ssl_certfile=ssl_certfile)


if __name__ == "__main__":
    generate_certificate()
    if not os.path.exists(ROBOT_FILE_SUMMARY):
        os.mkdir(ROBOT_FILE_SUMMARY) 

    file_path='23年地产行业动态-1月.docx'
    __spec__ = "ModuleSpec(name='builtins', loader=<class '_frozen_importlib.BuiltinImporter'>)"
    
    parser = argparse.ArgumentParser(prog='a unified parser',
                                 description='a unified parser')
    parser.add_argument("--host", type=str, default="0.0.0.0")
    parser.add_argument("--port", type=int, default=15282)
    # 初始化消息
    args = parser.parse_args()
    args_dict = vars(args)
#     api_start(args.host, args.port,ssl_keyfile="./key.pem", ssl_certfile="./cert.pem") #使用ssl 进行安全验证
    api_start(args.host, args.port)

