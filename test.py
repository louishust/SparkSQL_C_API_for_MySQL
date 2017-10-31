class test(object):
    ll = "123"
    def p(self):
        print test.ll
        test.ll = "234"

a = test()
a.p()
print a.ll