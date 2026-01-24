from pathlib import Path

from storage import Storage


class Engine:
    def __init__ (self):
        self.storage = Storage("data/log.dat")
        self.index = {} #for a while it is 
        #self.path_index = Path("data/index.dat")
        #self.path_index.parent.mkdir(parents=True, exist_ok=True)
    

    def save_index(self,offset,key):
        self.index[key] = offset
        
        return
        #with open(self.path_index,'a') as f:
        #    f.write(key+" "+str(offset))
        
    def store_pair(self, key,value):
        self.save_index(self.storage.add(key,value),key) # the index dont save values, only pointers
        #self.save_index(self.storage.add('clave1','valor1'),key)
        #self.save_index(self.storage.add('clave2','valor2'),value)
        
    def read_by_offset(self,offset):
        self.storage.read_at(offset)
    
    def read_from_index(self,key):
        
        if not Path(self.storage.path).exists():
            return None
        if key not in self.index:
            return None
        self.read_by_offset(self.index[key])
        
        

if __name__ == "__main__":
    engine = Engine()
    engine.store_pair("usuario", "contraseña")
    engine.read_from_index("usuario")
