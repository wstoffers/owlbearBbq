#**    This line is 79 characters long.  The 80th character should wrap.   ***\

#imports:
import re
import pytest
from getBucketSize import gcsBucket

import subprocess
import numpy as np

#define:
@pytest.fixture
def rawDataLakeBucket():
    '''Returns a bucket for the raw data lake'''
    return gcsBucket('gs://wstoffers-galvanize-owlbear-data-lake-raw')

def test_transformOutput_raisesOnErr(rawDataLakeBucket):
    out = ''
    err = 'hello universe'
    with pytest.raises(RuntimeError):
        rawDataLakeBucket._transformOutput(out, err)

def test_transformOutput_transform(rawDataLakeBucket):
    err = ''
    out = b'23081749  anyString'
    assert rawDataLakeBucket._transformOutput(out, err) == 23081749

def test_getSize_string(rawDataLakeBucket):
    class pOpen(object):
        def __init__(self, strings, *args, **kwargs):
            self.strings = strings
        def communicate(self):
            return self.strings
    
    setattr(rawDataLakeBucket,'_transformOutput',lambda *args: ' '.join(args))
    setattr(subprocess,'Popen',pOpen)
    expected = 'gsutil du -s gs://'
    m = re.match(expected,rawDataLakeBucket._getSize(rawDataLakeBucket.buckets))
    assert m

def test_formString_si(rawDataLakeBucket):
    filtered = np.array([26.822252, np.inf])
    sizes = [26822252, 0]
    keys = np.array([2., -np.inf])
    ending = 'B' #or 'iB'
    strings = rawDataLakeBucket._formString(filtered,sizes,keys,ending)
    assert strings == ['26.82 MB', '0 B']

def test_formString_iec(rawDataLakeBucket):
    filtered = np.array([25.57969284, np.inf])
    sizes = [26822252, 0]
    keys = np.array([2., -np.inf])
    ending = 'iB'
    strings = rawDataLakeBucket._formString(filtered,sizes,keys,ending)
    assert strings == ['25.58 MiB', '0 B']

def test_withUnitsFormStringIntegration_mega(rawDataLakeBucket):
    sizes = [29881909, 0]
    array = rawDataLakeBucket._withUnits(sizes)
    assert array[0][0] == '29.88 MB'
    assert array[0][1] == '28.50 MiB'
    assert array[1][0] == '0 B'
    assert array[1][1] == '0 B'

def test_withUnitsFormStringIntegration_kilo(rawDataLakeBucket):
    sizes = [81909, 0]
    array = rawDataLakeBucket._withUnits(sizes)
    #np.testing.assert_almost_equal(array,
    #                               np.array([['29.88 MB', '28.50 MiB'],
    #                                         ['0 B','0 B']]))
    #^^^only use this with arrays of floats/doesn't work with strings
    assert array[0][0] == '81.91 KB'
    assert array[0][1] == '79.99 KiB'
    assert array[1][0] == '0 B'
    assert array[1][1] == '0 B'

def test_withUnitsFormStringIntegration_byte(rawDataLakeBucket):
    pass

def test_withUnitsFormStringIntegration_giga(rawDataLakeBucket):
    pass

def test_withUnitsFormStringIntegration_tera(rawDataLakeBucket):
    pass

#run:
if __name__ == '__main__':
    pass