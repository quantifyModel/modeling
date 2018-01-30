# -*- coding: utf-8 -*-

import os

class File:

    file_dir_content = {}
    file_dir_content["root_name"] = []
    current_dir = ""

    def __init__(self,current_dir):
        for root, dirs, files in os.walk(current_dir):
            self.file_dir_content[root] ={}
            self.file_dir_content[root]["dirs"] = dirs
            self.file_dir_content[root]["files"] = files
            self.file_dir_content[root]["files_path"] = [root+"/"+file for file in files]
            self.file_dir_content["root_name"].append(root)
            self.current_dir = current_dir

    def get_file_names_in_current_dir(self):
        return self.file_dir_content[self.current_dir]["files"]

    def get_file_names_with_path_in_current_dir(self):
        return self.file_dir_content[self.current_dir]["files_path"]

    def get_dir_names_in_current_dir(self):
        return self.file_dir_content[self.current_dir]["dirs"]

    def get_root_name(self):
        return self.file_dir_content["root_name"]



# demo

f = File("../data")

print (f.get_file_names_with_path_in_current_dir())