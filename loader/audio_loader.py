"""Loader that loads image files."""
from typing import List

from langchain.document_loaders.unstructured import UnstructuredFileLoader
import torch
from transformers import AutoModelForSpeechSeq2Seq, AutoProcessor, pipeline
import os
import nltk


class UnstructuredAudioLoader(UnstructuredFileLoader):
    """Loader that uses unstructured to load image files, such as PNGs and JPGs."""

    def _get_elements(self) -> List:
        def audio_to_txt(filepath, dir_path="tmp_files"):

            WHISPER__PATH="/data/whisper-large-v3"
            device = 'cuda' if torch.cuda.is_available() else "cpu"
            torch_dtype = torch.float16 if torch.cuda.is_available() else torch.float32
            model_id =WHISPER__PATH 
            model = AutoModelForSpeechSeq2Seq.from_pretrained(
                model_id, torch_dtype=torch_dtype, low_cpu_mem_usage=True, use_safetensors=True
            )
            model.to(device)
            processor = AutoProcessor.from_pretrained(model_id)
            pipe = pipeline(
                "automatic-speech-recognition",
                model=model,
                tokenizer=processor.tokenizer,
                feature_extractor=processor.feature_extractor,
                max_new_tokens=128,
                chunk_length_s=30,
                batch_size=16,
                return_timestamps=True,
                torch_dtype=torch_dtype,
                device=device,
            )   
            
            full_dir_path = os.path.join(os.path.dirname(filepath), dir_path)
            if not os.path.exists(full_dir_path):
                os.makedirs(full_dir_path)
            filename = os.path.split(filepath)[-1]
            result = pipe(filepath)['text']
            txt_file_path = os.path.join(full_dir_path, "%s.txt" % (filename))
            with open(txt_file_path, 'w', encoding='utf-8') as fout:
                fout.write(result)
            return txt_file_path

        txt_file_path = audio_to_txt(self.file_path)
        from unstructured.partition.text import partition_text
        return partition_text(filename=txt_file_path, **self.unstructured_kwargs)
      
      
if __name__ == "__main__":
    import sys
    filepath="tts.mp3"
    loader = UnstructuredAudioLoader(filepath, mode="elements")
    docs = loader.load()
    for doc in docs:
        print(doc)
