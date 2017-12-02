a=1234567.89
print("%06.03e" % a)
print("%04.06e" % a)
print("%04.05e" % a)
b=-2.756377345246e-6
print("%04.05e" % b)
b=-2.756377345246e-124
print("%04.05e" % b)
b=-2.756377345246e+124
print("%04.05e" % b)
c=["%04.05e" % b, "%04.05e" % a]
print(c)
print(c[1])
print(c[:])
print(c[:][0])
print(c[0])
print(c[0][0])
print(c[:][0])
print([x[0] for x in c])
d=[x[0] for x in c]
print(d)
print(d=='-')
print(d=[x[0]=='-' for x in c])
print(d)
