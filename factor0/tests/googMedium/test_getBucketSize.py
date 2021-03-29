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
