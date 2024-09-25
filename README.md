# 统一文档解析器
从langchain_chatchat的开源项目中分离出来，并增强了一些功能。可以解析word、pdf、图片、音频文件等,并统一解析成txt 文件，对pdf 文件可以按照顺序解析出图和表的内容(表格中合并单元格处理需要更改源代码),其中音频文件解析使用的是whisper-large-v3,需要放在/data 目录下（参考loader/audio_loader.py 文件）
注意对于分栏等有版式的pdf、图片解析效果一般

## 模型环境配置说明
一个conda环境，同时安装torch和paddle,要注意版本适配，并提前测试，可参考文档https://wenku.csdn.net/answer/6d8a634d6eae45a3a808c09229bbf0f0


# # 启动说明
python api_parser.py 即可启动程序,解析器具体实现在loader 文件夹里