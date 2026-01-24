import os

from pathlib import Path

from storage import Storage
import struct


class Engine:
    def __init__ (self):
        self.storage = Storage("data/log.dat")
        self.index = {} #for a while it is 
        self.compact_path = Path("data/log.compact.dat")
        self.compact_path.parent.mkdir(parents=True, exist_ok=True)
        
        #self.path_index = Path("data/index.dat")
        #self.path_index.parent.mkdir(parents=True, exist_ok=True)
        self._recover()
        
    def _recover(self):
        try:
            with open(self.storage.path,'rb') as f:
                while True:
                    offset = f.tell() # store current position
                    first_read = f.read(4)
                    if len(first_read) < 4:
                        break
                    key_sz = struct.unpack(">I",first_read)[0]
                    
                    sec_read  = f.read(4)
                    if len(sec_read) < 4:
                        break 
                    value_sz = struct.unpack(">I",sec_read)[0]
                    
                    third_read = f.read(key_sz)
                    if len(third_read) < key_sz:
                        break 
                    
                    four_read = f.read(value_sz)
                    if len(four_read) < value_sz:
                        break 
                    
                    key = third_read.decode('utf-8')
                    
                    self.index[key] = offset
        except FileNotFoundError:
            pass

    def compact(self):
        new_index ={}
        new_storage = Storage(str(self.compact_path))
        for k,o in self.index.items():
            result = self.read_by_index(k)
            if result is None:
                continue
            kv, vv = result
            new_index[kv] = new_storage.add(kv,vv)
            print(f'{self.index[kv]}\n')
            
            
        os.replace(self.compact_path,self.storage.path) # atomic replacement of names (pointer the content now), dleete the old log content
        self.index = new_index
        self.storage = new_storage
    
    def save_index(self,offset,key):
        self.index[key] = offset
        return
        #with open(self.path_index,'a') as f:
        #    f.write(key+" "+str(offset))
        
    def store_pair(self, key,value):
        offset = self.storage.add(key,value)
        self.save_index(offset,key) # the index dont save values, only pointers
        #self.save_index(self.storage.add('clave1','valor1'),key)
        #self.save_index(self.storage.add('clave2','valor2'),value)
        
        
    def read_by_offset(self,offset):
        return self.storage.read_at(offset)
    
    def read_by_index(self,key):
        
        if not Path(self.storage.path).exists():
            return None
        if key not in self.index:
            return None
        return self.read_by_offset(self.index[key])
        
        

if __name__ == "__main__":
    engine = Engine()
    #engine.store_pair("usuario", "contraseña6")
    #engine._recover()
    #engine.compact()
    #engine.read_by_index("usuario")
    engine.read_by_offset(14)
    
    engine.read_by_offset(34)