import sys

value_get = sys.argv[1]
value_expected = sys.argv[2]

if value_get == value_expected:
    print("Input type is correct")
else:
    raise ValueError(f"Expect {value_expected}; get {value_get}")
