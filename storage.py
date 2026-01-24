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
            first_read = f.read(4)
            if len(first_read) < 4:
                raise IOError("truncated record")
            key_sz = struct.unpack(">I",first_read)[0]
            
            sec_read  = f.read(4)
            if len(sec_read) < 4:
                raise IOError("truncated record")
            value_sz = struct.unpack(">I",sec_read)[0]
            
            third_Read = f.read(key_sz)
            if len(third_Read) < key_sz:
                raise IOError("truncated record")
            key_val = third_Read.decode('utf-8')
            
            four_read = f.read(value_sz)
            if len(four_read) < value_sz:
                raise IOError("truncated record")
            value_val = four_read.decode('utf-8')
            
        
            print(key_val + value_val)
            return key_val, value_val
        


