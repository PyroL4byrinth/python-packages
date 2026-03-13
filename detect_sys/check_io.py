import os, sys, io  
print("exe:", sys.executable)  
print("cwd:", os.getcwd())  
print("io:", io.__file__)  
print("text_encoding:", getattr(io, "text_encoding", "MISSING"))  