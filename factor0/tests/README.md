Test directory structure selected to be
```
src/
    
tests/
    __init__.py
    googSmall/
        __init__.py
        test_...
    googMedium/
        __init__.py
        test_...
    googLarge/
        __init__.py
        test_...
```
based on
- [Pytest Good Practices](https://docs.pytest.org/en/latest/explanation/goodpractices.html#test-discovery)
- [This Opinion Piece](https://blog.ionelmc.ro/2014/05/25/python-packaging/#the-structure)
- [And This Opinion Piece](https://hynek.me/articles/testing-packaging/)<br/>

and test size naming convention adopted from [this Google practice](https://testing.googleblog.com/2010/12/test-sizes.html).