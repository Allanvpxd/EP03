import time
from mrjob.job import MRJob
from mrjob.step import MRStep
from calendar import timegm

class MediaValor(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.avgmapper,
                   combiner=self.avgreducer,
                   reducer=self.avgreducer),
            MRStep(mapper=self.ttmapper,
                   combiner=self.ttreducer,
                   reducer=self.ttreducer) ]

    def avgmapper(self, _, line):
        words=line.split(',')
        if words[16] == "total_amount":
                print("cabecalho")
        else:
                corridafim=words[2]
                corridavalor=float(words[16])
                #EX: "2019-01-01 00:46:40"
                cordt=corridafim.split(' ')
                cordate=cordt[0]
                cortime=cordt[1]
                cortm=cortime.split(':')
                corh=cortm[0]
                corm=int(cortm[1])
                cors=cortm[2]
                cormin=""
                if ( corm < 15 ):
                        cormin="00"
                if ( corm > 14 and corm < 30 ):  
                        cormin="15"    
                if ( corm > 29 and corm < 45 ):  
                        cormin="30"    
                if ( corm > 44 ):  
                        cormin="45"    
                v_cortime=cordate+" "+corh+":"+cormin
#               print(corridadia, corridatime)
#               yield corridafim, (cordate, cortime, corh, corm, cors, cormin)
                yield v_cortime, (corridavalor,1)

    def avgreducer(self, key, values):
        s = 0
        c = 0
        for average, count in values:
                s += average * count
                c += count
        if c > 1:
                self.increment_counter('stats', 'below1', 1)
        yield key, (s/c, c)

    def ttmapper(self, key, value):
        yield None, (key, value[0]) 

    def ttreducer(self, key, values):
        lista = []
        for average, dia in values:
            lista.append((dia, average))
            lista.sort()

        for dia, average in lista:
            yield None, (dia, average)

if __name__ == '__main__':
    MediaValor.run()
