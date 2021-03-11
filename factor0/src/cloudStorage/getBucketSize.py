#!/opt/miniconda3/bin/python
#**    This line is 79 characters long.  The 80th character should wrap.   ***\

#imports:
import subprocess
import numpy as np
import pandas as pd


#define:
class gcsBucket(object):
    """Wrapper for gsutil, to gather bucket metadata without fetching blobs

    This can take a long time if there are hundreds of thousands of blobs, so 
        for buckets that large, consider using Google's Monitoring GUI. Only 
        considers unit prefixes as large as tera, because if your bucket 
        should be measured in peta-anything, is it really still a student
        project?

    Args:
        buckets: iterable containing strings representing bucket URLs

    Attributes:
        buckets: iterable containing strings representing bucket URLs
        prefix: dictionary for unit prefix lookup, using log division result

    """
    
    def __init__(self,buckets):
        self.buckets = buckets
        self.prefix = {4.:'T',3.:'G',2.:'M',1.:'K',-np.inf:''}

    def tidySize(self):
        sizes = []
        for bucket in self.buckets:
            sizes.append(self._getSize(bucket))
        return self._makeTidy(sizes)

    def _makeTidy(self,sizes):
        return pd.DataFrame(self._withUnits(sizes),
                            index=self.buckets,
                            columns=['SI','IEC']).to_markdown()

    def _withUnits(self,sizes):
        arrays = []
        for block, ending in ((1000,'B'),(1024,'iB')):
            expanded = np.tile(np.array([sizes]).transpose(),(1,4))
            blocks = np.array([[block**4,block**3,block**2,block]])
            inUnits = expanded/blocks
            with np.errstate(divide='ignore'):
                filtered = np.where(inUnits>1,inUnits,np.inf).min(axis=1)
                keys = np.log(np.array(sizes)/filtered)/np.log(block)
            arrays.append(self._formString(filtered,sizes,keys,ending))
        return np.column_stack(arrays)

    def _formString(self, reduction, bites, keys, originalEnding):
        #use bites instead of bytes to avoid stomping on built-in
        forArray = []
        for reduced, byte, key in zip(reduction, bites, keys):
            if reduced == np.inf:
                reduced, ending, format = byte, 'B', 'd'
            else:
                format, ending = '.2f', originalEnding
            forArray.append(f'{reduced:{format}} {self.prefix[key]}{ending}')
        return forArray
    
    def _getSize(self, bucket):
        process = subprocess.Popen(['gsutil',
                                    'du',
                                    '-s',
                                    bucket],
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        return self._transformOutput(*process.communicate())

    def speakFrench(self):
        print('Est-il mignon de mettre un chat dans un seau?')

    def _transformOutput(self, out, err):
        if err:
            raise RuntimeError(err)
        if out:
            return int(out.decode('utf8').split()[0])

#run:
if __name__ == '__main__':
    maid = gcsBucket(['gs://wstoffers-galvanize-owlbear-data-lake-raw',
                      'gs://wstoffers-galvanize-owlbear-data-lake-transformed'])
    print(maid.tidySize())
