#!/usr/bin/env python3
"""
Simple test script to verify naming-standards package functionality
"""

def test_transforms_names():
    """Test the naming-standards package basic functionality"""
    print("Testing naming-standards package...")
    
    # Test imports
    try:
        from naming_standards import Tablename, Colname, ColList, NamedList
        print("✓ All imports successful")
    except ImportError as e:
        print(f"✗ Import failed: {e}")
        return False
    
    # Test Tablename
    try:
        t1 = Tablename("my_table")
        t2 = Tablename("_private_table") 
        t3 = Tablename("123")
        assert str(t1) == "my_table"
        assert str(t2) == "_private_table"
        assert str(t3) == "123"
        print("✓ Tablename tests passed")
    except Exception as e:
        print(f"✗ Tablename test failed: {e}")
        return False
    
    # Test Colname
    try:
        c1 = Colname("CustomerName")
        c2 = Colname("order_123")
        assert str(c1) == "customername"
        assert str(c2) == "order_123"
        print("✓ Colname tests passed")
    except Exception as e:
        print(f"✗ Colname test failed: {e}")
        return False
    
    # Test ColList
    try:
        cl = ColList(["customerid", "ordernumber", "amount123"])
        assert len(cl) == 3
        assert "customerid" in cl
        print("✓ ColList tests passed")
    except Exception as e:
        print(f"✗ ColList test failed: {e}")
        return False
    
    # Test NamedList
    try:
        nl = NamedList(["Item1", "Item2"])
        assert str(nl) == "NamedList(['item1', 'item2'])"
        print("✓ NamedList tests passed")
    except Exception as e:
        print(f"✗ NamedList test failed: {e}")
        return False
    
    print("✓ All naming-standards tests passed!")
    return True

if __name__ == "__main__":
    success = test_transforms_names()
    exit(0 if success else 1)