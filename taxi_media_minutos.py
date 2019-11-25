import time
from mrjob.job import MRJob
from mrjob.step import MRStep
from calendar import timegm

class MediaMinutos(MRJob):
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
                corrdia=words[1].split(' ')
                corridadia=corrdia[0]
                corridaori=words[1]
                corridafim=words[2]
                #EX: "2019-01-01 00:46:40"
                utcori = time.strptime(corridaori, "%Y-%m-%d %H:%M:%S")
                epochori = timegm(utcori)
                utcfim = time.strptime(corridafim, "%Y-%m-%d %H:%M:%S")
                epochfim = timegm(utcfim)
                corridatime=(epochfim - epochori)/60
#               print(corridadia, corridatime)
                yield corridadia, (corridatime,1)

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
    MediaMinutos.run()