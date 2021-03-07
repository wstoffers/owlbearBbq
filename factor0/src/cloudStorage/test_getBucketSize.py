#**    This line is 79 characters long.  The 80th character should wrap.   ***\

#imports:
import pytest
from getBucketSize import gcsBucket

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







#run:
if __name__ == '__main__':
    pass
