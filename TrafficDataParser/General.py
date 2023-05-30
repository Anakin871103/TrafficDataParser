
import os
import numpy as np

class General():

    def __init__(self):
        self.name = 0
    
    def get_current_directory_path() -> str:
        BASE_DIRECTORY = os.path.dirname(os.path.abspath(__file__))
        return BASE_DIRECTORY

