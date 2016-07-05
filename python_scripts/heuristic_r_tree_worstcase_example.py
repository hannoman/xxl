"""Constructs the worst-case example for heuristic R-Trees from the Priority-R-Tree paper."""

import fractions

F = fractions.Fraction

def lcm(a,b):
    if a == 0:
        return b
    if b == 0:
        return a
    else:
        return a*b / fractions.gcd(a,b)

def revbits(z,k):
    return int("".join(reversed(bin(z)[2:].rjust(k, "0")[-k:])), 2)
    

def heuristic_r_tree_worstcase_example(N, B, k=2, fracs=0):
    ps = [] # needs transposal
    
    for i in range(N//B):
        ps += [[]]
        if fracs: x = i + F(1,2)
        else:     x = i + 1/2
        for j in range(B):
            if fracs: y = F(j,B) + F(revbits(i,k),N)
            else:     y = j / B + revbits(i,k) / N
            ps[-1] += [(x,y)]
    
    return ps
    
def multed(N,B,k=2):
    ps = heuristic_r_tree_worstcase_example(N, B, k, fracs=1)
    
    m = 1 # lcm of all
    for col in ps:
        for point in col:            
            m = lcm(m, point[0])
            m = lcm(m, point[1])
            
    ps_ = [ [(int(p[0]*m), int(p[1]*m)) for p in col] for col in ps]
    return ps_