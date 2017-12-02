def f0():
    print('f0')

def f1():
    print('f1')

def f2():
    print('f2')

myFuncs = {423:f0, 3215:f1, 2543:f2}
myFuncs.get(3215, f0)()
myFuncs.get(3216, f0)()
