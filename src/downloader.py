'''
Modus Operandi:
Get Download Link from User
Start Download -> Depending upon system allowance, create thread with chunk limits
'''
import requests
import logging
import os 
from concurrent.futures import ThreadPoolExecutor
import math
import time 
class Downloader:
    '''
        TODO:
        - Get Link from User
        - Obtain Get Request and divide chunks
        - Assign Chunks to Each Thread and Initiate Thread (what is acceptable for the system)
        - Store splits when done
        - Combine splits and compare CRC/SHA256 hash
    '''
    def __init__(self, path: str):
        self.path = path  
        self.logger = logging.getLogger(__name__)
        # check if a splitting directory is created or no 

        self.output_dir = ".temp"
        os.makedirs(self.output_dir, exist_ok=True)


    def getDownloadRequest(self):
        '''
            Obtain Download Request from the server, and once obtained check for hash [OPTIONAL].
            If present store it for reference.
        '''
        try:
            response = requests.get(
                url=self.path,
            )

            self.file_name = self.getFileName(self.path)

            # accept headers from the file
            file_headers = response.headers

            # Check if the downloaded can be in form of streams
            if 'Accept-Ranges' in file_headers:
                self.logger.info(msg=f"Headers Acquired, Starting to download the file {self.file_name}")
                self.file_headers = file_headers
            
            else:
            # obtain strategy for downloading non streamable
                pass 

        except Exception as e:
            self.logger.error(msg=f"An exception has occured, while retrieving the file: {e}")
    
    def getFileName(self, url: str):
        return url.strip('/').split('/')[-1]
    
    def initializeDownload(self, max_workers: int):
        try:
            total_bytes = self.file_headers['Content-Length']
            chunk_size = self.getChunkSize(total_bytes)

            #  Content-Length/Max Chunk Size = Number of Chunks
            self.total_chunk_length = math.ceil(total_bytes / chunk_size)

            with ThreadPoolExecutor(max_workers = max_workers) as executor:
                for i in range(self.total_chunk_length):
                    start = i * chunk_size
                    end = min(start + chunk_size - 1, total_bytes - 1)
                    executor.submit(
                        self.downloadSplit,
                        start, end, f"part{i}", chunk_size, i + 1
                    )

        except Exception as e:
            self.logger.error(f"Failed to Initialize TPE {e}")
         
    
    def downloadSplit(self, start: int, end: int, part_id: str, chunk_size: int, section: int):
        '''
            Download split by start and end of the bytes specified
        '''
        try:
            self.logger.info(msg=f"Beginning to download {part_id}, {section}/{self.total_chunk_length}")
            # add range header to specify which chunk window is needed to be downloaded
            headers = {'Range': f'bytes={start}-{end}'}
            split = requests.get(
                url = self.path,
                headers=headers,
                stream=True
            )
            with open(f"{self.output_dir}/{part_id}",'wb') as f:
                for chunk in split.iter_content(chunk_size):
                    f.write(chunk)

        except Exception as e:
            self.logger.error(msg=f"Download failed on {part_id}: {e}")


    def combineSplits(self):
        # not happy with method chatgpt gave
        with open(self.file_name, 'wb') as outfile:
            for i in range(self.total_chunk_length):
                part_file = f"{self.output_dir}/part{i}"
                with open(part_file, 'rb') as pf:
                    outfile.write(pf.read())

    def displayProgressStatus(self):
        pass 

    def getChunkSize(self, total_size: int):
        '''
            Devise or find algorithm to find optimal chunk size and ensure lower number of chunks[DSA!!]
        '''
        pass

    def getMaxWorkers(self):
        pass 



