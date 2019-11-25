from mrjob.job import MRJob
from mrjob.step import MRStep

class CobrancasDia(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer)
#            MRStep(mapper=self.mapper)
        ]

    def mapper(self, _, line):
        words=line.split(',')
        if words[16] == "total_amount":
                print("cabecalho")
        else:
                corrdia=words[1].split(' ')
                corridadia=corrdia[0]
                corridavalor=float(words[16])
        
                yield corridadia, corridavalor

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    CobrancasDia.run()