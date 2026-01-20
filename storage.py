import struct
from pathlib import Path

class Storage():
    def __init__(self, path: str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        
    def add(self,key,value):
        key_bytes = key.encode('utf-8')
        value_bytes = value.encode('utf-8') # necesary for serialization, transmission net , bbdd
        
        with  open(self.path,'ab') as f:
            offset = f.tell() # pointer to disk
            record = (
                    struct.pack(">I",len(key_bytes)) +
                    struct.pack(">I",len(value_bytes)) +
                    key_bytes +
                    value_bytes
                 )
            f.write(record)
            print(record)
            f.flush() # principle of durability (D of ACID)
            return offset
    
    def read_at(self,offset):
        
        with open(self.path, 'rb') as f:
            f.seek(offset) # random access to disk
            key_sz = struct.unpack(">I",f.read(4))[0]
            value_sz = struct.unpack(">I",f.read(4))[0]
            
            key_val = f.read(key_sz).decode('utf-8')
            value_val = f.read(value_sz).decode('utf-8')
            
        
            print(key_val + value_val)
        


