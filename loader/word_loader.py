"""word 解析，使用spire.doc库"""
import pdb
from typing import List

from langchain.document_loaders.unstructured import UnstructuredFileLoader
from typing import IO, Any, Callable, Dict, List, Optional, Sequence, Union
import os,json
from loguru import logger

from spire.doc import *
from spire.doc.common import *

       

class UnstructuredWordLoader(UnstructuredFileLoader):
    def __init__(
        self,       
        file_path: Union[str, List[str]],
        mode: str = "single",
        pages:bool=False,

        **unstructured_kwargs:Any,
    ):
        self.pages=pages
        super().__init__(file_path=file_path,mode=mode, **unstructured_kwargs)        
    """Loader that uses unstructured to load image files, such as PNGs and JPGs."""
        

    def _get_elements(self) -> List:  
        info_other="Evaluation Warning: The document was created with Spire.Doc for Python."
               
        def word_ocr_txt(filepath, dir_path="tmp_files",to_image=False):
            full_dir_path = os.path.join(os.path.dirname(filepath), dir_path)
            if not os.path.exists(full_dir_path):
                os.makedirs(full_dir_path)                     
            txt_file_path = os.path.join(full_dir_path, f"{os.path.split(filepath)[-1]}.txt")
            img_name = os.path.join(full_dir_path, f"{os.path.split(filepath)[-1]}.png")
            
            try:
                document_text=""
                document = Document()
                document.LoadFromFile(filepath)
                document_text = document.GetText()
                document_text=document_text.replace(info_other,"")  
                
                
                
                #处理图片
                import queue
                nodes = queue.Queue()
                nodes.put(document)

                #创建Image列表
                images = []

                #遍历
                while nodes.qsize() > 0:
                    node = nodes.get()
                    for i in range(node.ChildObjects.Count):
                        child = node.ChildObjects.get_Item(i)
                        if child.DocumentObjectType == DocumentObjectType.Picture:
                            picture = child if isinstance(child, DocPicture) else None
                            dataBytes = picture.ImageBytes
                            images.append(dataBytes)
                        elif isinstance(child, ICompositeObject):
                            nodes.put(child if isinstance(child, ICompositeObject) else None)                
                #将图像数据写入到文件
                if len(images)>0:
                    from paddleocr import PaddleOCR
                    ocr = PaddleOCR(use_angle_cls=True, lang="ch", use_gpu=True, show_log=False)                  
                for i, item in enumerate(images):
                    with open(img_name,'wb') as imageFile:
                        imageFile.write(item)  
                    try:                      
                        result_temp = ocr.ocr(img_name)
                    except Exception as e:
                        continue
                    result_temp2=[]
                    for line in result_temp:
                        if line==None or not line or len(line)==0: #为None
                            continue
                        else:
                            result_temp2.append(line)
                    ocr_result = [i[1][0]+"\n" for line in result_temp2 for i in line] 
                    document_text=document_text+"".join(ocr_result)
    
                with open(txt_file_path, 'w', encoding='utf-8') as fout:
                    fout.write(document_text) #写入文件  
            except Exception as e: #加密或无效   
                logger.error("pdf_ocr_txt error:{0}".format(e))
                from unstructured.partition.text import element_from_text
                from unstructured.documents.elements import Element,ElementMetadata

                element = element_from_text("")
                element.metadata=ElementMetadata(filename=txt_file_path)
                return [element] #返回只有1个元素的列表                 
            if os.path.exists(img_name):
                os.remove(img_name)
            return txt_file_path
        
        
        def word_ocr_txt_pages(filepath, dir_path="tmp_files",to_image=False):
            #word 暂未实现按照页码来进行解析
            full_dir_path = os.path.join(os.path.dirname(filepath), dir_path)
            if not os.path.exists(full_dir_path):
                os.makedirs(full_dir_path)                     
            txt_file_path = os.path.join(full_dir_path, f"{os.path.split(filepath)[-1]}.txt")
            img_name = os.path.join(full_dir_path, f"{os.path.split(filepath)[-1]}.png")
            result={}
            try:
                document_text=""
                document = Document()
                document.LoadFromFile(filepath)
                document_text = document.GetText()
                document_text=document_text.replace(info_other,"")  
                
                
                
                #处理图片
                import queue
                nodes = queue.Queue()
                nodes.put(document)

                #创建Image列表
                images = []

                #遍历
                while nodes.qsize() > 0:
                    node = nodes.get()
                    for i in range(node.ChildObjects.Count):
                        child = node.ChildObjects.get_Item(i)
                        if child.DocumentObjectType == DocumentObjectType.Picture:
                            picture = child if isinstance(child, DocPicture) else None
                            dataBytes = picture.ImageBytes
                            images.append(dataBytes)
                        elif isinstance(child, ICompositeObject):
                            nodes.put(child if isinstance(child, ICompositeObject) else None)                
                #将图像数据写入到文件
                if len(images)>0:
                    from paddleocr import PaddleOCR
                    ocr = PaddleOCR(use_angle_cls=True, lang="ch", use_gpu=True, show_log=False)                    
                for i, item in enumerate(images):
                    with open(img_name,'wb') as imageFile:
                        imageFile.write(item)  
                    try:                      
                        result_temp = ocr.ocr(img_name)
                    except Exception as e:
                        continue    

                    result_temp2=[]
                    for line in result_temp:
                        if line==None or not line or len(line)==0: #为None
                            continue
                        else:
                            result_temp2.append(line)
                    ocr_result = [i[1][0]+"\n" for line in result_temp2 for i in line] 
                    document_text=document_text+ "".join(ocr_result)
               
                result["page"]=document_text     
                with open(txt_file_path, 'w', encoding='utf-8') as fout:
                    fout.write(json.dumps(result,ensure_ascii=False)) #写入文件                
            except Exception as e: #加密或无效  
                logger.error("word_ocr_txt_pages error:{0}".format(e))
                from unstructured.partition.text import element_from_text
                from unstructured.documents.elements import Element,ElementMetadata

                element = element_from_text("")
                element.metadata=ElementMetadata(filename=txt_file_path)
                return [element] #返回只有1个元素的列表 

                      
            if os.path.exists(img_name):
                os.remove(img_name)
            return txt_file_path
        if self.pages: #带页码
            txt_file_path = word_ocr_txt_pages(self.file_path)
        else:
            txt_file_path = word_ocr_txt(self.file_path)
        if isinstance(txt_file_path,str): 
#             from unstructured.partition.text import partition_text
#             return partition_text(filename=txt_file_path, **self.unstructured_kwargs)
            return txt_file_path
        elif isinstance(txt_file_path,list):
            return txt_file_path


if __name__ == "__main__":
    import sys
    filepath="../test/青岛市关于2024年度全国一、二级注册建筑师资格考试的通知.doc"
    print(os.path.exists(filepath))
    loader = UnstructuredWordLoader(filepath, mode="elements")
    txt_file_path=loader._get_elements()
#     docs = loader.load()
#     for doc in docs:
#         print(doc)
