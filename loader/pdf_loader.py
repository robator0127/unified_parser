"""Loader that loads image files.,先使用pdfplumber 进行文字解析，如解析为空，则使用ocr解析page.images的图片,对于分栏则使用fitz进行解析"""
import pdb
from typing import List

from langchain.document_loaders.unstructured import UnstructuredFileLoader
from typing import IO, Any, Callable, Dict, List, Optional, Sequence, Union
import os,json
import fitz
import pdfplumber #第二个pdf 转图片的库
from tqdm import tqdm
from loguru import logger

   
    

class UnstructuredPaddlePDFLoader(UnstructuredFileLoader):
    def __init__(
        self,       
        file_path: Union[str, List[str]],
        mode: str = "single",
        pages:bool=False,
        enhanced:bool=True,#是否使用pdfplumber
        **unstructured_kwargs:Any,
    ):
        self.pages=pages
        self.enhanced=enhanced
        super().__init__(file_path=file_path,mode=mode, **unstructured_kwargs)        
    """Loader that uses unstructured to load image files, such as PNGs and JPGs."""
    
    def _get_elements(self) -> List:
        def taketop(element):
            return element[2]
        def get_table(table):#获取表格的内容，table 为list
            table_result=""
            row_line=""#标题行
            for index,temp in enumerate(table):
                if any(temp2==None for temp2 in temp)==True:#有None类型:
                    if all(temp2==None for temp2 in temp)==True:#全部为None
                        continue 
                else:
                    line=" ".join(temp)
                    if not line.replace(" ",""):#去除空格后无内容，跳出
                        continue
                    else:
                        if not row_line:#row_line 不存在
                            row_line=temp #标题行
                            continue #跳出循环
                if row_line:#已找到row_line，则处理
            #             print(row_line)
                    #合并数据
                    temp_result=""
                    if len(row_line)!=len(temp):#列数不一致
                       return ""
                    if any(temp2==None for temp2 in temp)==True:#有None类型

                        for j,temp2 in enumerate(temp):
                                if j+1<len(temp):
                                    if temp[j]==None and temp[j+1]==None: #一行内。连续2个None，则不再处理
                                        temp[j]=""
                                        temp[j+1]=""
                                        continue                
                                if temp2==None:
                                    if j ==0:

                                        if index>0:#取上一条数据
                                            temp[j]=table[index-1][j]
                                        else:
                                            temp[j]=""
                                    else:#取左边数据
                                        #判断是否None是否连着

                                        temp[j]=temp[j-1]

                    for j in range(len(row_line)):
                        temp_result=temp_result+row_line[j]+":"+temp[j] +" "
                    table_result=table_result+temp_result.replace("\n","")+"\n"   
            return table_result      
        
        
        def pdf_ocr_txt(filepath, dir_path="tmp_files",to_image=False):
            full_dir_path = os.path.join(os.path.dirname(filepath), dir_path)
            if not os.path.exists(full_dir_path):
                os.makedirs(full_dir_path)                     
            txt_file_path = os.path.join(full_dir_path, f"{os.path.split(filepath)[-1]}.txt")
            img_name = os.path.join(full_dir_path, f"{os.path.split(filepath)[-1]}.png")
#             pdf_info=pdfplumber.open(filepath)
            doc = fitz.open(filepath)
            if not to_image:
                with open(txt_file_path, 'w', encoding='utf-8') as fout:
                    for i in range(doc.page_count):
                        text=""
                        try:
                            page = doc[i]
                            text = page.get_text("")
                        except Exception as e: #加密或无效           
                            from unstructured.partition.text import element_from_text
                            from unstructured.documents.elements import Element,ElementMetadata
                            
                            element = element_from_text("")
                            element.metadata=ElementMetadata(filename=txt_file_path)
                            return [element] #返回只有1个元素的列表                                               
                        
                        if '�' in text:#有乱码
                            return pdf_ocr_txt(filepath, dir_path="tmp_files",to_image=True) #全部使用OCR 解析
                        fout.write(text)
                        fout.write("\n")

                        img_list = page.get_images()
                        for img in img_list:
                            pix = fitz.Pixmap(doc, img[0])
                            if pix.n - pix.alpha >= 4:
                                pix = fitz.Pixmap(fitz.csRGB, pix)
                            pix.save(img_name)
                            from paddleocr import PaddleOCR
                            ocr = PaddleOCR(use_angle_cls=True, lang="ch", use_gpu=True, show_log=False)
                            result = ocr.ocr(img_name)
                            result2=[]
                            for line in result:
                                if line==None or not line or len(line)==0: #为None
                                    continue  
                                else:
                                    result2.append(line)                            
                            ocr_result = [i[1][0] for line in result2 for i in line]
                            fout.write("\n".join(ocr_result))
            else:
                #图片形式解析
                from paddleocr import PaddleOCR
                ocr = PaddleOCR(use_angle_cls=True, lang="ch", use_gpu=True, show_log=False)
                with open(txt_file_path, 'w', encoding='utf-8') as fout:
                    for i in range(doc.page_count):
                        page = doc[i]   
                        pm = page.get_pixmap(dpi=300) #整页转化为图片,时间较长
                        pm.save(img_name)
                        result = ocr.ocr(img_name)
                        result2=[]
                        for line in result:
                            if line==None or not line or len(line)==0: #为None
                                continue  
                            else:
                                result2.append(line)                        
                        ocr_result = [i[1][0] for line in result2 for i in line]
                        fout.write("\n".join(ocr_result))                          
            if os.path.exists(img_name):
                os.remove(img_name)
            return txt_file_path
        
        def pdf_ocr_txt_enhanced(filepath, dir_path="tmp_files",to_image=False): #默认false
            full_dir_path = os.path.join(os.path.dirname(filepath), dir_path)
            if not os.path.exists(full_dir_path):
                os.makedirs(full_dir_path)                     
            txt_file_path = os.path.join(full_dir_path, f"{os.path.split(filepath)[-1]}.txt")
            img_name = os.path.join(full_dir_path, f"{os.path.split(filepath)[-1]}.png")
            if not to_image:#不全部转化为图片
                if os.path.exists(txt_file_path):
                   os.remove(txt_file_path)
                with open(txt_file_path, 'w', encoding='utf-8') as fout:
                    try:
                        with pdfplumber.open(filepath) as pdf_file:
                            for i in tqdm(range(len(pdf_file.pages))):
                                page_result=""
                                new_lines=[]
                                page = pdf_file.pages[i]
                                #处理表格
                                try:
                                    tables = page.extract_tables()
                                    table_objects=page.find_tables() #表格位置信息
                                except Exception as e:
                                    logger.info("page.extrace_tables error:{0}".format(e))
                                    tables=[]
                                    table_objects=[]
                                if len(tables)==0:#无表格,使用extract_text()
                                    for temp in page.extract_text_lines():
                                        new_lines.append((temp["text"],temp["x0"],temp["top"],temp["x1"],temp["bottom"]))
                                        if '�' in temp["text"] or "\x00" in temp["text"]:
                                            return pdf_ocr_txt_pages(filepath, dir_path="tmp_files",to_image=True) #全部使用OCR 解析


    #                                 page_content = page.extract_text()
    #                                 if page_content: #有数据
    #                                     page_result=page_result+page_content
    #                                     if '�' in page_content or "\x00" in page_content:#有乱码
    #                                         return pdf_ocr_txt_pages(filepath, dir_path="tmp_files",to_image=True) #全部使用OCR 解析
                                else:
                                    #表格分析
                                    new_lines=[] #先清空原来的
                                    for index,table in enumerate(tables):
                                        table_result=get_table(table)
                                        table_location=table_objects[index].bbox
                                        if table_result:#有内容，要替换
                                            new_lines.append((table_result,table_location[0],table_location[1],table_location[2],table_location[3]))#x0,top,x1,bottom
                                            for temp in page.extract_text_lines():
                                                if temp["x0"]>=table_location[0] and temp["top"]>=table_location[1] and temp["x1"]<=table_location[2] and temp["bottom"]<=table_location[3]:
                                                    continue
                                                else:
                                                    new_lines.append((temp["text"],temp["x0"],temp["top"],temp["x1"],temp["bottom"]))
                                        else:#无内容
                                            for temp in page.extract_text_lines():
                                                new_lines.append((temp["text"],temp["x0"],temp["top"],temp["x1"],temp["bottom"]))
                                                                                                          
                                                        
                                #不再看有没有数据都分析图片,图片分析
                                img_list = page.images
                                if len(img_list)>0:
                                    from paddleocr import PaddleOCR
                                    ocr = PaddleOCR(use_angle_cls=True, lang="ch", use_gpu=True, show_log=False)                                 
                                for temp in img_list:
                                    new_coordinate=[min(page.bbox[2],max(0,temp["x0"])),min(page.bbox[3],max(0,temp["top"])),min(page.bbox[2],temp["x1"]),min(page.bbox[3],temp["bottom"])]
                                    if new_coordinate[0]==new_coordinate[2] or new_coordinate[1] == new_coordinate[3]:
                                        continue #无效，退出循环                                    
                                    page_crop=page.crop(new_coordinate)
                                    pm = page_crop.to_image(resolution=300)
                                    pm.save(img_name)
                                    result_temp = ocr.ocr(img_name)
                                    result_temp2=[]
                                    for line in result_temp:
                                        if line==None or not line or len(line)==0: #为None
                                            continue
                                        else:
                                            result_temp2.append(line)
                                    ocr_result = [i[1][0]+"\n" for line in result_temp2 for i in line]
                                    new_lines.append(("".join(ocr_result),new_coordinate[0],new_coordinate[1],new_coordinate[2],new_coordinate[3]))#x0,top,x1,bottom

                                #去重
                                new_lines=list(set(new_lines))
                                new_lines.sort(key=taketop)
                                #获取总结果
                                for temp in new_lines:
                                    page_result=page_result+temp[0]+"\n"
             
                                fout.write(page_result+"\n")                     
                    except Exception as e: #加密或无效   
                        logger.error("pdf_ocr_txt_enhanced error:{0}".format(e))
                        from unstructured.partition.text import element_from_text
                        from unstructured.documents.elements import Element,ElementMetadata

                        element = element_from_text("")
                        element.metadata=ElementMetadata(filename=txt_file_path)
                        return [element] #返回只有1个元素的列表 
                    
            else:
                #图片形式解析  
                try:
                    with pdfplumber.open(filepath) as pdf_file:
                        for i in range(len(pdf_file.pages)):
                            page_result=""
                            page = pdf_file.pages[i]
                            page_content = page.extract_text()  
                except Exception as e: #加密或无效 :
                    from unstructured.partition.text import element_from_text
                    from unstructured.documents.elements import Element,ElementMetadata

                    element = element_from_text("")
                    element.metadata=ElementMetadata(filename=txt_file_path)
                    return [element] #返回只有1个元素的列表                
                from paddleocr import PaddleOCR
                ocr = PaddleOCR(use_angle_cls=True, lang="ch", use_gpu=True, show_log=False)
                with open(txt_file_path, 'w', encoding='utf-8') as fout: 
                    for page in tqdm(pdf_info.pages):   
                        pm = page.to_image(resolution=300) #整页转化为图片,1500够了
                        pm.save(img_name)
                        result = ocr.ocr(img_name)
                        result2=[]
                        for line in result:
                            if line==None or not line or len(line)==0: #为None
                                continue  
                            else:
                                result2.append(line)
                        ocr_result = [i[1][0] for line in result2 for i in line]
                        fout.write("\n".join(ocr_result))                        
            if os.path.exists(img_name):
                os.remove(img_name)
            return txt_file_path
        
        def pdf_ocr_txt_pages(filepath, dir_path="tmp_files",to_image=False):
#             pdb.set_trace()
            #使用pdfplumber进行pages功能的实现
            full_dir_path = os.path.join(os.path.dirname(filepath), dir_path)
            if not os.path.exists(full_dir_path):
                os.makedirs(full_dir_path)                     
            txt_file_path = os.path.join(full_dir_path, f"{os.path.split(filepath)[-1]}.txt")
            img_name = os.path.join(full_dir_path, f"{os.path.split(filepath)[-1]}.png")
            result={}
            if not to_image:#不全部转化为图片                
                try:                    
                    with pdfplumber.open(filepath) as pdf_file:
                        for i in tqdm(range(len(pdf_file.pages))):
                            page_result=""
                            new_lines=[]
                            page = pdf_file.pages[i]
                            #处理表格
                            try:
                                tables = page.extract_tables()
                                table_objects=page.find_tables() #表格位置信息
                            except Exception as e:
                                logger.info("page.extrace_tables error:{0}".format(e))
                                tables=[] #出错，不再处理
                                table_object=[]
                            if len(tables)==0:#无表格,使用extract_text()
                                for temp in page.extract_text_lines():
                                    new_lines.append((temp["text"],temp["x0"],temp["top"],temp["x1"],temp["bottom"]))
                                    if '�' in temp["text"] or "\x00" in temp["text"]:
                                        return pdf_ocr_txt_pages(filepath, dir_path="tmp_files",to_image=True) #全部使用OCR 解析
                                        
                                    
#                                 page_content = page.extract_text()
#                                 if page_content: #有数据
#                                     page_result=page_result+page_content
#                                     if '�' in page_content or "\x00" in page_content:#有乱码
#                                         return pdf_ocr_txt_pages(filepath, dir_path="tmp_files",to_image=True) #全部使用OCR 解析
                            else:
                                #表格分析
                                new_lines=[] #先清空原来的
                                for index,table in enumerate(tables):
                                    table_result=get_table(table)
                                    table_location=table_objects[index].bbox
                                    if table_result:#有内容，要替换
                                        new_lines.append((table_result,table_location[0],table_location[1],table_location[2],table_location[3]))#x0,top,x1,bottom
                                                                                
                                        for temp in page.extract_text_lines():
                                            if temp["x0"]>=table_location[0] and temp["top"]>=table_location[1] and temp["x1"]<=table_location[2] and temp["bottom"]<=table_location[3]:
                                                continue
                                            else:
                                                new_lines.append((temp["text"],temp["x0"],temp["top"],temp["x1"],temp["bottom"]))
                                    else: #无内容，全部添加
                                        for temp in page.extract_text_lines():
                                            new_lines.append((temp["text"],temp["x0"],temp["top"],temp["x1"],temp["bottom"]))
                                            
                                            

                                                          
                                                        
                            #不再看有没有数据都分析图片,图片分析          
                            img_list = page.images
                            if len(img_list)>0:
                                from paddleocr import PaddleOCR
                                ocr = PaddleOCR(use_angle_cls=True, lang="ch", use_gpu=True, show_log=False)                               
                            for temp in img_list:
                                try:
                                    new_coordinate=[min(page.bbox[2],max(0,temp["x0"])),min(page.bbox[3],max(0,temp["top"])),min(page.bbox[2],temp["x1"]),min(page.bbox[3],temp["bottom"])]
                                    if new_coordinate[0]==new_coordinate[2] or new_coordinate[1] == new_coordinate[3]:
                                        continue #无效，退出循环
                                    page_crop=page.crop(new_coordinate)
                                    pm = page_crop.to_image(resolution=300)
                                    pm.save(img_name)
                                    result_temp = ocr.ocr(img_name)
                                    result_temp2=[]
                                    for line in result_temp:
                                        if line==None or not line or len(line)==0: #为None
                                            continue
                                        else:
                                            result_temp2.append(line)
                                    ocr_result = [i[1][0]+"\n" for line in result_temp2 for i in line]
                                    new_lines.append(("".join(ocr_result),new_coordinate[0],new_coordinate[1],new_coordinate[2],new_coordinate[3]))#x0,top,x1,bottom
                                except:
                                    continue
                            new_lines=list(set(new_lines))#去重
                            new_lines.sort(key=taketop)
                            #获取总结果
                            for temp in new_lines:
                                page_result=page_result+temp[0]+"\n"
             
                            result["page_"+str(i+1)]=page_result
                except Exception as e: #加密或无效 
                    logger.error("pdf_ocr_txt_pages error:{0}".format(e))
                    from unstructured.partition.text import element_from_text
                    from unstructured.documents.elements import Element,ElementMetadata

                    element = element_from_text("")
                    element.metadata=ElementMetadata(filename=txt_file_path)
                    return [element] #返回只有1个元素的列表 
                with open(txt_file_path, 'w', encoding='utf-8') as fout:
                    fout.write(json.dumps(result,ensure_ascii=False)) #写入文件
            else:
                #图片形式解析
                try:
                    with pdfplumber.open(filepath) as pdf_file:
                        for i in range(len(pdf_file.pages)):
                            page_result=""
                            page = pdf_file.pages[i]
                            page_content = page.extract_text()  
                except Exception as e: #加密或无效 :
                    from unstructured.partition.text import element_from_text
                    from unstructured.documents.elements import Element,ElementMetadata

                    element = element_from_text("")
                    element.metadata=ElementMetadata(filename=txt_file_path)
                    return [element] #返回只有1个元素的列表                     
                from paddleocr import PaddleOCR
                ocr = PaddleOCR(use_angle_cls=True, lang="ch", use_gpu=True, show_log=False)
                for i,page in tqdm(enumerate(pdf_file.pages)):   
                    pm = page.to_image(resolution=300) #整页转化为图片,1500够了
                    pm.save(img_name)
                    result_ = ocr.ocr(img_name)
                    for temp in result_:
                        if temp==None or not temp or len(temp)==0: #为None
                            continue                    
                    ocr_result = [i[1][0]+"\n" for line in result_ for i in line]
                    result["page_"+str(i+1)]="".join(ocr_result)
                with open(txt_file_path, 'w', encoding='utf-8') as fout: 
                        json.dump(result,fout,ensure_ascii=False) #直接写入文件，并取消编码                       
            if os.path.exists(img_name):
                os.remove(img_name)
            return txt_file_path
        if self.pages: #带页码
            txt_file_path = pdf_ocr_txt_pages(self.file_path)
        else:
            txt_file_path = pdf_ocr_txt_enhanced(self.file_path)
        if not self.enhanced:#基本模式不带页码，即fitz 解析不带页码
            txt_file_path = pdf_ocr_txt(self.file_path)
        if isinstance(txt_file_path,str): 
#             from unstructured.partition.text import partition_text
#             return partition_text(filename=txt_file_path, **self.unstructured_kwargs)
            return txt_file_path
        elif isinstance(txt_file_path,list):
            return txt_file_path


if __name__ == "__main__":
    import sys
    # sys.path.append(os.path.dirname(os.path.dirname(__file__)))
    # filepath = os.path.join(os.path.dirname(os.path.dirname(__file__)), "knowledge_base", "samples", "content", "test.pdf")
    filepath="中国科学院2023研究前沿研判128个科学研究前言.pdf"
    print(os.path.exists(filepath))
    loader = UnstructuredPaddlePDFLoader(filepath, mode="elements",pages=True)
    docs = loader.load()
    for doc in docs:
        print(doc)
