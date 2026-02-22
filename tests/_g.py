import os
Q = chr(34)
Q3 = 3*Q
N = chr(10)
D = os.path.dirname(os.path.abspath(__file__))
def w(name, lines):
    p = os.path.join(D, name)
    t = N.join(lines).replace('QQQ',Q3).replace('QQ',Q)
    open(p,'w',encoding='utf-8').write(t)
    print(f'Written {len(t)} chars to {p}')
 
# Use pipe as line separator for compact representation 
def ws(name, s): 
    lines = s.split('PIPE') 
    w(name, lines)
