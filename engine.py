from storage import Storage


storage = Storage("data/log.dat")

storage.read_at(storage.add('clave1','valor1'))
storage.read_at(storage.add('clave2','valor2'))


